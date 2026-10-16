# Changes

## 2026-10-01

- Consolidated VAR–Lighter configuration into one canonical template set; removed duplicate profiles and optional extra-symbol definitions.
- Aligned monitoring, pairing and segmented strategies to BTC, ETH, BNB, SOL and HYPE.
- Both segmented and ordinary V3 strategy templates now default to `system_mode.monitor_only: true`. Enable live execution explicitly in the corresponding configuration only after account, request-template and budget validation.
- Preserved retained per-symbol sizing, segmented risk settings and order settings. Removed execution settings for exchanges outside the delivery scope.
- Restored six VAR request templates and public account/WS settings through explicitly authorized local redaction. Authentication headers, cookies and personal identifiers remain removed. Historical request validity still requires user verification.
