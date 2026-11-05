//! The text of panics on this thread, while a run is collecting them.
//!
//! Async drives some streams from detached tasks, and a panic in one reaches
//! nothing that awaits it: the panic hook is the only place that sees its text.

std::thread_local! {
    static PANIC_TEXTS: std::cell::RefCell<Option<Vec<String>>> =
        const { std::cell::RefCell::new(None) };
}

/// Collect this thread's panics, and keep them off the terminal, until
/// [`stop`]. Other threads' panics print as usual.
pub fn start() {
    static HOOK: std::sync::Once = std::sync::Once::new();
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let text = info
                .payload()
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| info.payload().downcast_ref::<&str>().copied())
                .unwrap_or("<non-string payload>")
                .to_owned();
            let collected = PANIC_TEXTS.with_borrow_mut(|texts| match texts {
                Some(texts) => {
                    texts.push(text);
                    true
                }
                None => false,
            });
            if !collected {
                previous(info);
            }
        }));
    });
    PANIC_TEXTS.set(Some(Vec::new()));
}

/// The panics since the last call, earliest first.
pub fn take() -> Vec<String> {
    PANIC_TEXTS.replace(Some(Vec::new())).unwrap_or_default()
}

pub fn stop() {
    PANIC_TEXTS.set(None);
}
