"""End-to-end integration test for the Pro endpoints' redaction guard.

Bypasses the live MCP server by constructing a minimal FastAPI app
that wires the same _AttestBody / VerifyRequest models to a stub
handler, plus the same guard logic. Verifies that:

  1. Properly redacted text passes (would proceed to LLM call)
  2. Text containing AHV / IBAN / EMAIL is REJECTED with 400
     ``client_redaction_incomplete``
  3. Both legacy field names (``draft_text`` / ``selected_text``) and
     new field names (``redacted_text``) are accepted
  4. The error response lists ``patterns_detected`` (types only,
     never the matched substring — privacy by construction)
  5. Empty body is handled gracefully (no false 400)
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.testclient import TestClient

from quality.redact import is_likely_unredacted, redact as _server_redact


# Mirror the production models — kept locally here so the test runs
# without importing the full mcp_server (which has many dependencies).
class _AttestBody(BaseModel):
    redacted_text: str | None = None
    draft_text: str | None = None
    audit_grounding: bool = False
    client_redactor_version: str | None = None
    client_redactor_summary: dict | None = None


class _VerifyBody(BaseModel):
    license_key: str
    redacted_text: str | None = Field(default=None, max_length=5000)
    selected_text: str | None = Field(default=None, max_length=5000)
    case_ref: str = Field(..., max_length=200)
    lang: str = "de"
    client_redactor_version: str | None = None
    client_redactor_summary: dict | None = None


app = FastAPI()


def _guard(text: str, version: str | None) -> JSONResponse | str:
    """Returns the scrubbed text on pass, or a 400 JSONResponse on fail."""
    g = is_likely_unredacted(text)
    if not g.clean:
        return JSONResponse(
            {
                "error": "client_redaction_incomplete",
                "patterns_detected": g.patterns_found,
                "client_redactor_version": version,
            },
            status_code=400,
        )
    return _server_redact(text).redacted


@app.post("/attest")
async def attest(body: _AttestBody):
    text = body.redacted_text or body.draft_text or ""
    result = _guard(text, body.client_redactor_version)
    if isinstance(result, JSONResponse):
        return result
    return {"ok": True, "echo_length": len(result)}


@app.post("/billing/verify")
async def verify(req: _VerifyBody):
    text = req.redacted_text or req.selected_text or ""
    result = _guard(text, req.client_redactor_version)
    if isinstance(result, JSONResponse):
        return result
    return {"ok": True, "echo_length": len(result), "case_ref": req.case_ref}


client = TestClient(app)


# ────────────────────────────────────────────────────────────────────
# /attest
# ────────────────────────────────────────────────────────────────────

def test_attest_passes_properly_redacted_text():
    body = {
        "redacted_text": "Wie BGE 143 III 480 ausführt, hat [NAME_1] Anspruch.",
        "client_redactor_version": "redact.js@v3",
    }
    r = client.post("/attest", json=body)
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_attest_passes_pure_legal_prose():
    body = {"redacted_text": "Vgl. BGE 132 III 222, E. 4.2 i.V.m. Art. 41 OR."}
    r = client.post("/attest", json=body)
    assert r.status_code == 200


def test_attest_rejects_unredacted_ahv():
    body = {"redacted_text": "AHV-Nr. 756.1234.5678.90 wurde abgelegt."}
    r = client.post("/attest", json=body)
    assert r.status_code == 400
    payload = r.json()
    assert payload["error"] == "client_redaction_incomplete"
    assert "AHV" in payload["patterns_detected"]
    # Privacy-by-construction: the response must NOT contain the AHV substring
    assert "756.1234.5678.90" not in r.text


def test_attest_rejects_unredacted_iban():
    body = {"redacted_text": "Konto: CH93 0076 2011 6238 5295 7"}
    r = client.post("/attest", json=body)
    assert r.status_code == 400
    assert "IBAN" in r.json()["patterns_detected"]


def test_attest_rejects_unredacted_email():
    body = {"redacted_text": "Kontakt info@kanzlei.ch melden."}
    r = client.post("/attest", json=body)
    assert r.status_code == 400
    assert "EMAIL" in r.json()["patterns_detected"]


def test_attest_lists_multiple_pattern_hits():
    body = {"redacted_text": "info@x.ch und AHV 756.1111.2222.33"}
    r = client.post("/attest", json=body)
    assert r.status_code == 400
    assert set(r.json()["patterns_detected"]) >= {"EMAIL", "AHV"}


def test_attest_accepts_legacy_draft_text_field():
    """Older Word add-in versions that haven't picked up v=32 yet send
    the legacy field name. Guard still runs the same way."""
    body = {"draft_text": "Vgl. BGE 132 III 222."}
    r = client.post("/attest", json=body)
    assert r.status_code == 200


def test_attest_legacy_field_still_subject_to_guard():
    body = {"draft_text": "AHV 756.1111.2222.33"}
    r = client.post("/attest", json=body)
    assert r.status_code == 400
    assert "AHV" in r.json()["patterns_detected"]


def test_attest_handles_empty_body_gracefully():
    body = {"redacted_text": ""}
    r = client.post("/attest", json=body)
    assert r.status_code == 200


# ────────────────────────────────────────────────────────────────────
# /billing/verify
# ────────────────────────────────────────────────────────────────────

def test_verify_passes_redacted_text():
    body = {
        "license_key": "ocl_pro_test",
        "redacted_text": "Vgl. BGE 132 III 222.",
        "case_ref": "BGE 132 III 222",
    }
    r = client.post("/billing/verify", json=body)
    assert r.status_code == 200


def test_verify_rejects_unredacted_ahv():
    body = {
        "license_key": "ocl_pro_test",
        "redacted_text": "AHV 756.1234.5678.90",
        "case_ref": "BGE 132 III 222",
    }
    r = client.post("/billing/verify", json=body)
    assert r.status_code == 400
    assert "AHV" in r.json()["patterns_detected"]


def test_verify_accepts_legacy_selected_text_field():
    body = {
        "license_key": "ocl_pro_test",
        "selected_text": "Vgl. BGE 132 III 222.",
        "case_ref": "BGE 132 III 222",
    }
    r = client.post("/billing/verify", json=body)
    assert r.status_code == 200


def test_verify_rejects_when_both_fields_unredacted():
    body = {
        "license_key": "ocl_pro_test",
        "selected_text": "AHV 756.0000.0000.00",
        "case_ref": "BGE 132 III 222",
    }
    r = client.post("/billing/verify", json=body)
    assert r.status_code == 400


def test_verify_response_does_not_leak_pii_in_error():
    """Even the error path must not reflect the matched PII back —
    only type labels."""
    body = {
        "license_key": "ocl_pro_test",
        "redacted_text": "AHV 756.5555.6666.77 und info@private.ch",
        "case_ref": "BGE 132 III 222",
    }
    r = client.post("/billing/verify", json=body)
    assert r.status_code == 400
    body_text = r.text
    assert "756.5555.6666.77" not in body_text
    assert "info@private.ch" not in body_text
    # but it should mention WHICH types triggered
    assert "AHV" in body_text
    assert "EMAIL" in body_text
