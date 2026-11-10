// Copyright 2019-2024 Tauri Programme within The Commons Conservancy
// SPDX-License-Identifier: Apache-2.0
// SPDX-License-Identifier: MIT

use crate::{Runtime, Webview};

use super::{EmitArgs, Event, EventId, EventTarget};

use std::{
  boxed::Box,
  cell::Cell,
  collections::{HashMap, HashSet},
  sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicU32, Ordering},
  },
};

/// What to do with the pending handler when resolving it?
enum Pending {
  Unlisten(EventId),
  Listen {
    id: EventId,
    event: crate::EventName,
    handler: Handler,
  },
  Emit(EmitArgs),
  RemoveForTarget(EventTarget),
}

impl Pending {
  fn apply(self, handlers: &mut Handlers) {
    match self {
      Pending::Unlisten(id) => handlers.values_mut().for_each(|handlers| {
        handlers.remove(&id);
      }),
      Pending::Listen { id, event, handler } => {
        handlers.entry(event).or_default().insert(id, handler);
      }
      Pending::Emit(emit_args) => {
        dispatch(handlers, &emit_args, &None::<&dyn Fn(&EventTarget) -> bool>)
      }
      Pending::RemoveForTarget(target) => {
        for handlers in handlers.values_mut() {
          handlers.retain(|_, handler| handler.target != target);
        }
        handlers.retain(|_, handlers| !handlers.is_empty());
      }
    }
  }
}

fn dispatch<F: Fn(&EventTarget) -> bool>(
  handlers: &Handlers,
  emit_args: &EmitArgs,
  filter: &Option<F>,
) {
  if let Some(handlers) = handlers.get(&emit_args.event) {
    for (&id, Handler { target, callback }) in handlers {
      if match_any_or_filter(target, filter) {
        callback(Event::new(id, emit_args.payload.clone()))
      }
    }
  }
}

/// Stored in [`Listeners`] to be called upon, when the event that stored it, is triggered.
struct Handler {
  target: EventTarget,
  callback: Box<dyn Fn(Event) + Send>,
}

impl Handler {
  fn new<F: Fn(Event) + Send + 'static>(target: EventTarget, callback: F) -> Self {
    Self {
      target,
      callback: Box::new(callback),
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct JsHandler {
  target: EventTarget,
  id: EventId,
}

impl JsHandler {
  fn new(target: EventTarget, id: EventId) -> Self {
    Self { target, id }
  }
}

type WebviewLabel = String;

type Handlers = HashMap<crate::EventName, HashMap<EventId, Handler>>;

/// Holds event handlers and pending event handlers, along with the salts associating them.
struct InnerListeners {
  pending: Mutex<Vec<Pending>>,
  handlers: Mutex<Handlers>,
  js_event_listeners: Mutex<HashMap<WebviewLabel, HashMap<crate::EventName, HashSet<JsHandler>>>>,
  function_name: &'static str,
  listeners_object_name: &'static str,
  next_event_id: Arc<AtomicU32>,
}

/// A self-contained event manager.
#[derive(Clone)]
pub struct Listeners {
  inner: Arc<InnerListeners>,
}

impl Default for Listeners {
  fn default() -> Self {
    Self {
      inner: Arc::new(InnerListeners {
        pending: Mutex::default(),
        handlers: Mutex::default(),
        js_event_listeners: Mutex::default(),
        function_name: "__internal_unstable_listeners_function_id__",
        listeners_object_name: "__internal_unstable_listeners_object_id__",
        next_event_id: Default::default(),
      }),
    }
  }
}

impl Listeners {
  pub(crate) fn next_event_id(&self) -> EventId {
    self.inner.next_event_id.fetch_add(1, Ordering::Relaxed)
  }

  /// Function name to represent the JavaScript event function.
  pub(crate) fn function_name(&self) -> &str {
    self.inner.function_name
  }

  /// Listener object name to represent the JavaScript event listener object.
  pub(crate) fn listeners_object_name(&self) -> &str {
    self.inner.listeners_object_name
  }

  fn pending(&self) -> MutexGuard<'_, Vec<Pending>> {
    self
      .inner
      .pending
      .lock()
      .expect("poisoned pending event queue")
  }

  /// Runs `action` on the handlers, or queues it while another call holds them.
  ///
  /// Handlers run with the handlers lock held, so calls made from a handler, or from another
  /// thread during a dispatch, are queued. Whoever holds the lock applies the queue before
  /// releasing it and checks it again afterwards, and whoever queues tries to apply it, so a
  /// queued action always has someone to apply it.
  fn run_or_queue<T>(
    &self,
    action: T,
    run: impl FnOnce(&mut Handlers, T),
    queued: impl FnOnce(T) -> Pending,
  ) {
    match self.inner.handlers.try_lock() {
      Ok(mut handlers) => {
        self.apply_pending(&mut handlers);
        run(&mut handlers, action);
        self.apply_pending(&mut handlers);
      }
      Err(_) => self.pending().push(queued(action)),
    }

    while !self.pending().is_empty() {
      let Ok(mut handlers) = self.inner.handlers.try_lock() else {
        return;
      };
      self.apply_pending(&mut handlers);
    }
  }

  fn run(&self, action: Pending) {
    self.run_or_queue(
      action,
      |handlers, action| action.apply(handlers),
      |action| action,
    );
  }

  fn apply_pending(&self, handlers: &mut Handlers) {
    loop {
      let pending = std::mem::take(&mut *self.pending());
      if pending.is_empty() {
        return;
      }
      for action in pending {
        action.apply(handlers);
      }
    }
  }

  /// Adds an event listener.
  pub(crate) fn listen<F: Fn(Event) + Send + 'static>(
    &self,
    event: crate::EventName,
    target: EventTarget,
    handler: F,
  ) -> EventId {
    let id = self.next_event_id();
    let handler = Handler::new(target, handler);
    self.run(Pending::Listen { id, event, handler });
    id
  }

  /// Listen to an event and immediately unlisten.
  pub(crate) fn once<F: FnOnce(Event) + Send + 'static>(
    &self,
    event: crate::EventName,
    target: EventTarget,
    handler: F,
  ) -> EventId {
    let self_ = self.clone();
    let handler = Cell::new(Some(handler));

    self.listen(event, target, move |event| {
      // This can potentially be called multiple times if the `unlisten` was queued,
      // see https://github.com/tauri-apps/tauri/issues/16214
      if let Some(handler) = handler.take() {
        self_.unlisten(event.id);
        handler(event);
      }
    })
  }

  /// Removes an event listener.
  pub(crate) fn unlisten(&self, id: EventId) {
    self.run(Pending::Unlisten(id));
  }

  /// Removes every Rust-side event listener registered for the exact given target.
  ///
  /// Called when a window or webview is destroyed so its listeners don't leak.
  /// Global targets such as [`EventTarget::App`], [`EventTarget::Any`] and [`EventTarget::AnyLabel`] are never matched here.
  fn remove_listeners_for_target(&self, target: EventTarget) {
    self.run(Pending::RemoveForTarget(target));
  }

  /// Emits the given event with its payload based on a filter.
  pub(crate) fn emit_filter<F>(&self, emit_args: EmitArgs, filter: Option<F>) -> crate::Result<()>
  where
    F: Fn(&EventTarget) -> bool,
  {
    self.run_or_queue(
      emit_args,
      |handlers, emit_args| dispatch(handlers, &emit_args, &filter),
      Pending::Emit,
    );
    Ok(())
  }

  /// Emits the given event with its payload.
  pub(crate) fn emit(&self, emit_args: EmitArgs) -> crate::Result<()> {
    self.emit_filter(emit_args, None::<&dyn Fn(&EventTarget) -> bool>)
  }

  pub(crate) fn listen_js(
    &self,
    event: crate::EventName<&str>,
    source_webview_label: &str,
    target: EventTarget,
    id: EventId,
  ) {
    let event = event.into_owned();
    let mut listeners = self.inner.js_event_listeners.lock().unwrap();
    listeners
      .entry(source_webview_label.to_string())
      .or_default()
      .entry(event)
      .or_default()
      .insert(JsHandler::new(target, id));
  }

  pub(crate) fn unlisten_js(&self, event: crate::EventName<&str>, id: EventId) {
    let event = event.into_owned();
    let mut js_listeners = self.inner.js_event_listeners.lock().unwrap();
    let js_listeners = js_listeners.values_mut();
    for js_listeners in js_listeners {
      if let Some(handlers) = js_listeners.get_mut(&event) {
        handlers.retain(|h| h.id != id);

        if handlers.is_empty() {
          js_listeners.remove(&event);
        }
      }
    }
  }

  /// Removes all JS event listeners registered from the given webview.
  fn remove_webview_js_listeners(&self, webview_label: &str) {
    self
      .inner
      .js_event_listeners
      .lock()
      .unwrap()
      .remove(webview_label);
  }

  /// Removes all event listeners associated with a destroyed webview.
  pub(crate) fn remove_webview_listeners(&self, label: &str) {
    self.remove_webview_js_listeners(label);
    self.remove_listeners_for_target(EventTarget::webview(label));
  }

  /// Removes all event listeners associated with a destroyed window.
  pub(crate) fn remove_window_listeners(&self, label: &str) {
    self.remove_listeners_for_target(EventTarget::window(label));
    self.remove_listeners_for_target(EventTarget::webview_window(label));
  }

  pub(crate) fn has_js_listener<F: Fn(&EventTarget) -> bool>(
    &self,
    event: crate::EventName<&str>,
    filter: F,
  ) -> bool {
    let event = event.into_owned();
    let js_listeners = self.inner.js_event_listeners.lock().unwrap();
    js_listeners.values().any(|events| {
      events
        .get(&event)
        .map(|handlers| handlers.iter().any(|handler| filter(&handler.target)))
        .unwrap_or(false)
    })
  }

  pub(crate) fn emit_js_filter<R, F>(
    &self,
    webviews: Vec<Webview<R>>,
    emit_args: &EmitArgs,
    filter: Option<F>,
  ) -> crate::Result<()>
  where
    R: Runtime,
    F: Fn(&EventTarget) -> bool,
  {
    let event = &emit_args.event;
    // `eval` can block until the main thread runs the script, so holding the listeners lock
    // while waiting deadlocks any main thread code that takes it (e.g. `emit`)
    let targets = {
      let js_listeners = self.inner.js_event_listeners.lock().unwrap();
      webviews
        .into_iter()
        .filter_map(|webview| {
          let handlers = js_listeners.get(webview.label())?.get(event)?;
          let ids = handlers
            .iter()
            .filter(|handler| match_any_or_filter(&handler.target, &filter))
            .map(|handler| handler.id)
            .collect::<Vec<_>>();
          Some((webview, ids))
        })
        .collect::<Vec<_>>()
    };

    targets
      .into_iter()
      .try_for_each(|(webview, ids)| webview.emit_js(emit_args, &ids))
  }

  pub(crate) fn emit_js<R>(
    &self,
    webviews: Vec<Webview<R>>,
    emit_args: &EmitArgs,
  ) -> crate::Result<()>
  where
    R: Runtime,
  {
    self.emit_js_filter(webviews, emit_args, None::<&dyn Fn(&EventTarget) -> bool>)
  }
}

#[inline(always)]
fn match_any_or_filter<F: Fn(&EventTarget) -> bool>(
  target: &EventTarget,
  filter: &Option<F>,
) -> bool {
  *target == EventTarget::Any || filter.as_ref().map(|f| f(target)).unwrap_or(true)
}

#[cfg(test)]
mod test {
  use super::*;
  use crate::event::EventTarget;
  use proptest::prelude::*;

  // dummy event handler function
  fn event_fn(s: Event) {
    println!("{s:?}");
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(10000))]

    // check to see if listen() is properly passing keys into the LISTENERS map
    #[test]
    fn listeners_check_key(e in "[a-z]+") {
      let listeners: Listeners = Default::default();
      // clone e as the key
      let key = crate::EventName::new(e).unwrap();
      // pass e and an dummy func into listen
      listeners.listen(key.clone(), EventTarget::Any, event_fn);

      // lock mutex
      let l = listeners.inner.handlers.lock().unwrap();

      // check if the generated key is in the map
      assert!(l.contains_key(&key));
    }

    // check to see if listen inputs a handler function properly into the LISTENERS map.
    #[test]
    fn listeners_check_fn(e in "[a-z]+") {
      let listeners: Listeners = Default::default();
      let key = crate::EventName::new(e).unwrap();
       // pass e and an dummy func into listen
       listeners.listen(key.clone(), EventTarget::Any, event_fn);

       // lock mutex
       let mut l = listeners.inner.handlers.lock().unwrap();

       // check if l contains key
       if l.contains_key(&key) {
        // grab key if it exists
        let handler = l.get_mut(&key);
        // check to see if we get back a handler or not
        match handler {
          // pass on Some(handler)
          Some(_) => {},
          // Fail on None
          None => panic!("handler is None")
        }
      }
    }

    // check to see if on_event properly grabs the stored function from listen.
    #[test]
    fn check_on_event(e in "[a-z]+", d in "[a-z]+") {
      let listeners: Listeners = Default::default();
      let key = crate::EventName::new(e).unwrap();
      // call listen with key and the event_fn dummy func
      listeners.listen(key.clone(), EventTarget::Any, event_fn);
      // call on event with key and d.
      listeners.emit(EmitArgs::new(key.as_str_event(), &d)?)?;

      // lock the mutex
      let l = listeners.inner.handlers.lock().unwrap();

      // assert that the key is contained in the listeners map
      assert!(l.contains_key(&key));
    }
  }

  #[test]
  fn event_no_deadlocks() {
    let listeners = Listeners::default();
    let listeners_clone = listeners.clone();
    let event = crate::EventName::new("test-event".to_owned()).unwrap();
    let event_clone = event.clone();
    listeners.once(event.clone(), EventTarget::Any, move |_event| {
      listeners_clone
        .emit(EmitArgs::new(event_clone.as_str_event(), &()).unwrap())
        .unwrap();
    });
    listeners
      .emit(EmitArgs::new(event.as_str_event(), &()).unwrap())
      .unwrap();
  }

  #[test]
  fn once_survives_a_replayed_queued_emit() {
    use std::sync::atomic::AtomicBool;

    let listeners = Listeners::default();
    let event = crate::EventName::new("event".to_owned()).unwrap();

    // Since the listeners are stored in a HashMap,
    // use more listens to make it more likely to have one of these to happen before the `once` handler later
    for _ in 0..100 {
      let listeners_clone = listeners.clone();
      let event_clone = event.clone();
      let emitted = AtomicBool::new(false);
      listeners.listen(event.clone(), EventTarget::Any, move |_| {
        // The listener lock is held while handlers run, so this emit is queued.
        if !emitted.swap(true, Ordering::SeqCst) {
          listeners_clone
            .emit(EmitArgs::new(event_clone.as_str_event(), &()).unwrap())
            .unwrap();
        }
      });
    }

    let calls = Arc::new(AtomicU32::new(0));
    let calls_clone = calls.clone();

    listeners.once(event.clone(), EventTarget::Any, move |_| {
      calls_clone.fetch_add(1, Ordering::SeqCst);
    });

    listeners
      .emit(EmitArgs::new(event.as_str_event(), &()).unwrap())
      .unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 1);
  }

  #[test]
  fn emit_queued_during_another_threads_dispatch_is_delivered() {
    use std::sync::mpsc;

    let listeners = Listeners::default();
    let filtered = crate::EventName::new("filtered".to_owned()).unwrap();
    let reply = crate::EventName::new("reply".to_owned()).unwrap();

    listeners.listen(filtered.clone(), EventTarget::App, event_fn);
    let delivered = Arc::new(AtomicU32::new(0));
    let delivered_clone = delivered.clone();
    listeners.listen(reply.clone(), EventTarget::Any, move |_| {
      delivered_clone.fetch_add(1, Ordering::SeqCst);
    });

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let dispatcher = {
      let listeners = listeners.clone();
      std::thread::spawn(move || {
        // The filter runs while the dispatching thread holds the handlers lock,
        // and rejects the only handler, so this dispatch calls no handler.
        let filter = |_: &EventTarget| {
          entered_tx.send(()).unwrap();
          release_rx.recv().unwrap();
          false
        };
        listeners
          .emit_filter(
            EmitArgs::new(filtered.as_str_event(), &()).unwrap(),
            Some(filter),
          )
          .unwrap();
      })
    };

    entered_rx.recv().unwrap();
    // The handlers are locked by the dispatcher, so this emit is queued.
    listeners
      .emit(EmitArgs::new(reply.as_str_event(), &()).unwrap())
      .unwrap();
    release_tx.send(()).unwrap();
    dispatcher.join().unwrap();

    assert_eq!(delivered.load(Ordering::SeqCst), 1);
  }

  #[test]
  fn listeners_removed_on_webview_close() {
    let listeners = Listeners::default();
    let event = crate::EventName::new("test-event".to_owned()).unwrap();
    let webview_label = "main";

    let window = listeners.listen(event.clone(), EventTarget::window(webview_label), event_fn);
    // Same label as the window, but a different target kind (multi-webview mode).
    let webview = listeners.listen(event.clone(), EventTarget::webview(webview_label), event_fn);
    let webview_window = listeners.listen(
      event.clone(),
      EventTarget::webview_window(webview_label),
      event_fn,
    );
    let other = listeners.listen(event.clone(), EventTarget::webview("other"), event_fn);
    let app = listeners.listen(event.clone(), EventTarget::App, event_fn);
    let any = listeners.listen(event.clone(), EventTarget::Any, event_fn);
    let labeled = listeners.listen(event.clone(), EventTarget::labeled(webview_label), event_fn);

    let id = listeners.next_event_id();
    listeners.listen_js(
      event.as_str_event(),
      webview_label,
      EventTarget::webview(webview_label),
      id,
    );
    assert!(listeners.has_js_listener(event.as_str_event(), |_| true));

    listeners.remove_webview_listeners(webview_label);

    assert!(!listeners.has_js_listener(event.as_str_event(), |_| true));

    let lock = listeners.inner.handlers.lock().unwrap();
    let remaining: HashSet<EventId> = lock.get(&event).unwrap().keys().copied().collect();

    // only the exact `Webview { label: "main" }` listener is gone
    assert!(!remaining.contains(&webview));
    // same label but different kind is kept
    assert!(remaining.contains(&window));
    assert!(remaining.contains(&webview_window));
    // unrelated and global listeners are kept
    assert!(remaining.contains(&other));
    assert!(remaining.contains(&app));
    assert!(remaining.contains(&any));
    assert!(remaining.contains(&labeled));
  }
}
