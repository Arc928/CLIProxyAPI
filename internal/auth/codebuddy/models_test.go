package codebuddy

import (
	"reflect"
	"testing"
)

func TestParseModelConfigUsesCLICatalogAndUpstreamCapabilities(t *testing.T) {
	models, errParse := ParseModelConfig([]byte(`{"code":0,"data":{
		"agents":[{"name":"ide","models":["glm-5.0"]},{"name":"cli","models":["hy3-x","kimi-k2.8-preview","hy3-x","disabled","missing"]}],
		"models":[
			{"id":"glm-5.0","name":"Old IDE model"},
			{"id":"disabled","disabled":true},
			{"id":"hy3-x","name":"Hy3","maxInputTokens":192000,"maxOutputTokens":64000,"supportsImages":true,"supportsReasoning":true,"onlyReasoning":true},
			{"id":"kimi-k2.8-preview","name":"Kimi-K2.8-Preview","maxInputTokens":1000000,"maxOutputTokens":64000,"supportsImages":true,"supportsReasoning":true,"reasoning":{"supportedEfforts":["low","high","max"],"canDisableThinking":true}}
		]}}`))
	if errParse != nil {
		t.Fatal(errParse)
	}
	if len(models) != 2 || models[0].ID != "hy3-x" || models[1].ID != "kimi-k2.8-preview" {
		t.Fatalf("unexpected CLI catalog: %+v", models)
	}
	hy3 := models[0]
	if hy3.DisplayName != "Hy3" || hy3.ContextLength != 192000 || hy3.MaxCompletionTokens != 64000 || hy3.Thinking == nil || hy3.Thinking.ZeroAllowed {
		t.Fatalf("unexpected Hy3 capabilities: %+v", hy3)
	}
	if !reflect.DeepEqual(hy3.Thinking.Levels, []string{"low", "high"}) {
		t.Fatalf("Hy3 paid variant lost baseline thinking levels: %+v", hy3.Thinking)
	}
	kimi := models[1]
	if kimi.ContextLength != 1000000 || kimi.MaxCompletionTokens != 64000 || kimi.Thinking == nil || !kimi.Thinking.ZeroAllowed {
		t.Fatalf("unexpected Kimi capabilities: %+v", kimi)
	}
	if !reflect.DeepEqual(kimi.Thinking.Levels, []string{"low", "high", "max"}) || !reflect.DeepEqual(kimi.SupportedInputModalities, []string{"text", "image"}) {
		t.Fatalf("unexpected Kimi thinking/modalities: %+v", kimi)
	}
}

func TestParseModelConfigOverridesKnownModelCapabilities(t *testing.T) {
	models, errParse := ParseModelConfig([]byte(`{"code":0,"data":{
		"agents":[{"name":"cli","models":["hy4-preview","glm-5.3"]}],
		"models":[
			{"id":"hy4-preview","maxInputTokens":1000000,"supportsReasoning":true,"reasoning":{"supportedEfforts":["high"],"canDisableThinking":false}},
			{"id":"glm-5.3","supportsImages":false,"supportsReasoning":false}
		]}}`))
	if errParse != nil {
		t.Fatal(errParse)
	}
	if models[0].ContextLength != 1000000 || !reflect.DeepEqual(models[0].Thinking.Levels, []string{"high"}) || models[0].Thinking.ZeroAllowed {
		t.Fatalf("upstream Hy4 capabilities were not applied: %+v", models[0])
	}
	if models[1].Thinking != nil || !reflect.DeepEqual(models[1].SupportedInputModalities, []string{"text"}) {
		t.Fatalf("explicit unsupported capabilities were not applied: %+v", models[1])
	}
}

func TestParseModelConfigRejectsUnavailableConfiguration(t *testing.T) {
	for _, body := range []string{
		`not JSON`,
		`{"code":10001,"data":{}}`,
		`{"code":0,"data":{"models":null}}`,
		`{"code":0,"data":{"agents":[{"name":"ide","models":["hy3"]}],"models":[{"id":"hy3"}]}}`,
		`{"code":0,"data":{"agents":[{"name":"cli","models":["missing"]}],"models":[{"id":"hy3"}]}}`,
	} {
		if models, errParse := ParseModelConfig([]byte(body)); errParse == nil || models != nil {
			t.Errorf("invalid catalog accepted: %s", body)
		}
	}
}
