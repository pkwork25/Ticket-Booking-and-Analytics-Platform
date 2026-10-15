import json
from pathlib import Path

WF_DIR = Path(__file__).resolve().parents[1] / "n8n" / "workflows"


def load_all():
    return {p.name: json.loads(p.read_text()) for p in sorted(WF_DIR.glob("*.json"))}


def test_workflows_are_valid_and_fully_connected():
    wfs = load_all()
    assert len(wfs) == 3
    for name, wf in wfs.items():
        nodes = {n["name"] for n in wf["nodes"]}
        targets = set()
        for src, conn in wf["connections"].items():
            assert src in nodes, (name, src)
            for branch in conn["main"]:
                for t in branch:
                    assert t["node"] in nodes, (name, t)
                    targets.add(t["node"])
        triggers = {n["name"] for n in wf["nodes"] if "webhook" in n["type"].lower() or "Trigger" in n["type"]}
        assert nodes - targets == triggers, (name, nodes - targets)


def test_webhooks_require_header_auth():
    for wf in load_all().values():
        for n in wf["nodes"]:
            if n["type"] == "n8n-nodes-base.webhook":
                assert n["parameters"]["authentication"] == "headerAuth"
                assert n["parameters"]["path"].startswith("ticketpulse-")


def test_llm_calls_go_through_openrouter_with_fallback_and_json_output():
    for fname, wf in load_all().items():
        for n in wf["nodes"]:
            if n["type"] == "n8n-nodes-base.httpRequest":
                assert n["parameters"]["url"] == "https://openrouter.ai/api/v1/chat/completions"
                assert n.get("retryOnFail") and n.get("onError") == "continueRegularOutput"
        code = " ".join(n["parameters"].get("jsCode", "") + n["parameters"].get("jsonBody", "") for n in wf["nodes"])
        if fname != "00_error_handler.json":
            assert "fallbackModels" in code and "json_object" in code
