---
"tauri": patch:bug
---

Fix an event emitted while another thread was dispatching an event sometimes not reaching Rust listeners until a later event was emitted. A `listen` or `unlisten` call made at that moment was delayed the same way.
