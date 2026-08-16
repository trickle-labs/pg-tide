# Test levels

pg_tide uses these terms for evidence:

| Level | Meaning |
|---|---|
| Unit | One function or module; no external process is required. |
| Contract | Request, response, serialization, or protocol shape against a local boundary or faithful stub. |
| Integration | At least two real components interact through their real protocol. |
| End-to-end | A documented public API flow reaches the final observable result through the real coordinator. |
| Chaos | A deliberate failure verifies a safety or recovery invariant. |

Cargo's `tests/` directory is a layout convention, not proof of integration or
end-to-end coverage. Release claims must name the level actually exercised.

