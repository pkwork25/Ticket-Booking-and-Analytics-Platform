"""Static checks for the CloudFormation templates (cfn-lint runs in CI as well).

Catches the mistakes that usually only show up at deploy time: dangling !Ref / !GetAtt,
${} placeholders that don't resolve, and cross-stack !ImportValue names nobody exports.
"""

import re
from pathlib import Path

import yaml

INFRA = Path(__file__).resolve().parents[1] / "infra"
TEMPLATES = sorted(INFRA.glob("*.yaml"))


class CfnLoader(yaml.SafeLoader):
    pass


def _tag(loader, suffix, node):
    name = "Fn::" + suffix if suffix not in ("Ref", "Condition") else suffix
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
        if suffix == "GetAtt":
            value = value.split(".", 1)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    return {name: value}


CfnLoader.add_multi_constructor("!", _tag)


def load(path):
    return yaml.load(path.read_text(), Loader=CfnLoader)


def walk(node):
    yield node
    if isinstance(node, dict):
        for v in node.values():
            yield from walk(v)
    elif isinstance(node, list):
        for v in node:
            yield from walk(v)


def exports():
    names = set()
    for t in TEMPLATES:
        for out in load(t).get("Outputs", {}).values():
            exp = out.get("Export", {}).get("Name")
            if exp:
                names.add(exp["Fn::Sub"])
    return names


def test_templates_parse_and_have_descriptions():
    assert len(TEMPLATES) == 4
    for t in TEMPLATES:
        doc = load(t)
        assert doc["AWSTemplateFormatVersion"] == "2010-09-09"
        assert doc.get("Description") and doc.get("Resources")


def test_refs_getatts_subs_and_conditions_resolve():
    for t in TEMPLATES:
        doc = load(t)
        known = set(doc.get("Parameters", {})) | set(doc["Resources"])
        resources = set(doc["Resources"])
        conditions = set(doc.get("Conditions", {}))
        for node in walk(doc):
            if not isinstance(node, dict):
                continue
            if "Ref" in node:
                ref = node["Ref"]
                assert ref in known or ref.startswith("AWS::"), f"{t.name}: !Ref {ref}"
            if "Fn::GetAtt" in node:
                assert node["Fn::GetAtt"][0] in resources, f"{t.name}: !GetAtt {node['Fn::GetAtt']}"
            if "Fn::Sub" in node:
                sub = node["Fn::Sub"]
                text, local = (sub, {}) if isinstance(sub, str) else (sub[0], sub[1])
                for var in re.findall(r"\$\{([^}!]+)\}", text):
                    base = var.split(".")[0]
                    assert base in known or base in local or var.startswith("AWS::"), f"{t.name}: ${{{var}}}"
            if "Fn::If" in node:
                assert node["Fn::If"][0] in conditions
        for name, res in doc["Resources"].items():
            if "Condition" in res:
                assert res["Condition"] in conditions, f"{t.name}: {name}"


def test_every_import_is_exported():
    available = exports()
    for t in TEMPLATES:
        for node in walk(load(t)):
            if isinstance(node, dict) and "Fn::ImportValue" in node:
                name = node["Fn::ImportValue"]["Fn::Sub"]
                assert name in available, f"{t.name} imports {name} which no stack exports"


def test_imports_only_from_earlier_stacks():
    order = {}
    for i, t in enumerate(TEMPLATES):
        for out in load(t).get("Outputs", {}).values():
            if "Export" in out:
                order[out["Export"]["Name"]["Fn::Sub"]] = i
    for i, t in enumerate(TEMPLATES):
        for node in walk(load(t)):
            if isinstance(node, dict) and "Fn::ImportValue" in node:
                assert order[node["Fn::ImportValue"]["Fn::Sub"]] < i, t.name


def test_data_stores_are_private_and_encrypted():
    data = load(INFRA / "02-data.yaml")["Resources"]
    assert data["RedshiftWorkgroup"]["Properties"]["PubliclyAccessible"] is False
    for b in ("RawArchiveBucket", "ArtifactsBucket"):
        props = data[b]["Properties"]
        assert all(props["PublicAccessBlockConfiguration"].values())
        assert props["BucketEncryption"]
    assert data["EventStream"]["Properties"]["StreamEncryption"]["EncryptionType"] == "KMS"
    assert data["AlertsQueue"]["Properties"]["FifoQueue"] is True
    assert "RedrivePolicy" in data["AlertsQueue"]["Properties"]
    # credentials only via Secrets Manager dynamic references
    ns = data["RedshiftNamespace"]["Properties"]
    assert "resolve:secretsmanager" in ns["AdminUserPassword"]["Fn::Sub"]
