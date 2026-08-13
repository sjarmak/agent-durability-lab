package quickstart

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/cookbooks/coding-agents/presentation"
)

func TestEmbeddedCatalogDefinesExactRecoveryTriad(t *testing.T) {
	t.Parallel()
	catalog, err := LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog(): %v", err)
	}
	if len(catalog.Scenarios) != 1 {
		t.Fatalf("scenarios = %d, want 1", len(catalog.Scenarios))
	}
	want := []struct {
		variant presentation.Variant
		verdict presentation.Verdict
		effects int
	}{
		{presentation.VariantUnfaulted, presentation.VerdictValidPass, 1},
		{presentation.VariantUnsafe, presentation.VerdictValidFail, 2},
		{presentation.VariantProtected, presentation.VerdictValidPass, 1},
	}
	for index, expected := range want {
		episode := catalog.Scenarios[0].Episodes[index]
		if episode.Variant != expected.variant || episode.Verdict != expected.verdict ||
			episode.Outcome.PhysicalEffectCount != expected.effects {
			t.Fatalf("episode %d = %#v, want %s/%s/%d effects", index, episode, expected.variant, expected.verdict, expected.effects)
		}
		if !strings.HasSuffix(episode.NativeHistory.ArchiveMember, "/workflow-history.json") {
			t.Fatalf("episode %d native history member = %q", index, episode.NativeHistory.ArchiveMember)
		}
	}
}

func TestVerifyTestReceiptsRequiresEveryUnfilteredAudit(t *testing.T) {
	t.Parallel()
	var valid strings.Builder
	for _, name := range RequiredTestReceipts() {
		fmt.Fprintf(&valid, `{"Action":"pass","Test":%q}`+"\n", name)
	}
	if err := VerifyTestReceipts(strings.NewReader(valid.String())); err != nil {
		t.Fatalf("VerifyTestReceipts(valid): %v", err)
	}

	tests := []struct {
		name string
		data string
		want string
	}{
		{name: "missing", data: strings.Replace(valid.String(), receiptLine(RequiredTestReceipts()[1], "pass"), "", 1), want: "missing pass receipt"},
		{name: "skip", data: valid.String() + receiptLine(RequiredTestReceipts()[0], "skip"), want: "skipped"},
		{name: "failure", data: valid.String() + receiptLine(RequiredTestReceipts()[0], "fail"), want: "failed"},
		{name: "malformed", data: valid.String() + "not-json\n", want: "decode"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := VerifyTestReceipts(strings.NewReader(test.data))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("VerifyTestReceipts() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestWriteSummaryNamesEvidenceAndResponsibility(t *testing.T) {
	t.Parallel()
	catalog, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := WriteSummary(&output, catalog); err != nil {
		t.Fatalf("WriteSummary(): %v", err)
	}
	for _, term := range []string{
		"UNFAULTED  valid-pass  1 physical effect",
		"UNSAFE     valid-fail  2 physical effects",
		"PROTECTED  valid-pass  1 physical effect",
		"Temporal:", "Application:", "Destination:", "Falsifier:",
		"workflow-history.json", "102 histories replayed",
	} {
		if !strings.Contains(output.String(), term) {
			t.Fatalf("summary lacks %q:\n%s", term, output.String())
		}
	}
}

func receiptLine(test, action string) string {
	return fmt.Sprintf(`{"Action":%q,"Test":%q}`+"\n", action, test)
}
