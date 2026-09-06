"""Keep the client reference complete and execute its example offline."""
import ast
import asyncio
import re
from pathlib import Path

import pytest
from mcp.types import CallToolResult, TextContent

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs/mcp/response-shapes.md"


def test_reference_covers_tool_inventory_and_output_schemas():
    source = ast.parse((ROOT / "mcp_server.py").read_text())
    declaration = next(n for n in source.body if isinstance(n, ast.FunctionDef) and n.name == "_list_tools")
    tools = {}
    for node in ast.walk(declaration):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "Tool":
            keywords = {k.arg: k.value for k in node.keywords}
            tools[ast.literal_eval(keywords["name"])] = "outputSchema" in keywords
    rows = re.findall(r"^\| `([^`]+)` \| ([^|]+) \|", DOC.read_text(), re.MULTILINE)
    assert len(rows) == len(dict(rows)), "duplicate reference rows"
    assert set(dict(rows)) == set(tools), "tool added/removed without updating the reference"
    for name, format_ in rows:
        assert ("structured" in format_) == tools[name], name
    # Classify the direct success return, not nested JSON error returns.
    handler = next(n for n in source.body if isinstance(n, ast.AsyncFunctionDef)
                   and n.name == "_handle_call_tool_inner")
    for branch in ast.walk(handler):
        if not isinstance(branch, ast.If) or not isinstance(branch.test, ast.Compare):
            continue
        comparison = branch.test
        if not isinstance(comparison.left, ast.Name) or comparison.left.id != "name":
            continue
        if len(comparison.comparators) != 1 or not isinstance(comparison.comparators[0], ast.Constant):
            continue
        name = comparison.comparators[0].value
        returns = [node for node in branch.body if isinstance(node, ast.Return)]
        if name not in tools or tools[name] or not returns:
            continue
        result = returns[-1].value
        if not isinstance(result, ast.List) or not result.elts or not isinstance(result.elts[0], ast.Call):
            continue
        text = next((k.value for k in result.elts[0].keywords if k.arg == "text"), None)
        serialized = isinstance(text, ast.Call) and ast.unparse(text.func) == "json.dumps"
        assert (dict(rows)[name].strip() == "JSON text") == serialized, name
    assert "response-shapes.md" in (ROOT / "docs/mcp/index.html").read_text()


def example():
    code = re.search(r"```python\n(.*?)\n```", DOC.read_text(), re.DOTALL).group(1)
    scope = {}
    # Execute only this repository-owned example, never user-supplied text.
    exec(compile(code, str(DOC), "exec"), scope)  # noqa: S102
    return scope["top_citations"]


class Session:
    def __init__(self, search, citations):
        self.search = search
        self.citations = citations
        self.calls = []

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        payload = self.search if name == "search_decisions" else self.citations[arguments["reference"]]
        return CallToolResult(structuredContent=payload, isError=False,
                              content=[TextContent(type="text", text="Human text is not the citation source")])


def test_example_returns_stored_citations_without_reformatting():
    citations = {str(i): {"exists": True, "citation_string_de": f"stored field {i}"} for i in range(4)}
    session = Session({"results": [{"decision_id": str(i)} for i in range(4)]}, citations)
    assert asyncio.run(example()(session, "query")) == ["stored field 0", "stored field 1", "stored field 2"]
    assert len(session.calls) == 4


def test_example_empty_search_is_an_empty_list():
    assert asyncio.run(example()(Session({"results": []}, {}), "query")) == []


@pytest.mark.parametrize("payload", [None, {}, {"error": "timeout"}])
def test_example_rejects_missing_or_failed_structured_search(payload):
    with pytest.raises(RuntimeError):
        asyncio.run(example()(Session(payload, {}), "query"))


@pytest.mark.parametrize("citation", [{"exists": False}, {"exists": True}, {"exists": True, "citation_string_de": None}])
def test_example_never_constructs_missing_citations(citation):
    with pytest.raises(RuntimeError):
        asyncio.run(example()(Session({"results": [{"decision_id": "id"}]}, {"id": citation}), "query"))
