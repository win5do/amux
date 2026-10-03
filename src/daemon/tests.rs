use super::*;

fn fixture() -> (tempfile::TempDir, Daemon, Process) {
    let root = tempfile::tempdir().unwrap();
    let daemon = Daemon::new(Config {
        state_dir: root.path().join("state"),
        socket: root.path().join("daemon.sock"),
    })
    .unwrap();
    daemon.store.ensure_schema().unwrap();
    let workspace = daemon
        .store
        .create_workspace(CreateWorkspaceRequest {
            name: "dev".into(),
            ..Default::default()
        })
        .unwrap();
    let process = daemon
        .store
        .create_process(
            &workspace,
            RunRequest {
                name: "worker".into(),
                command: vec!["/usr/bin/true".into()],
                ..Default::default()
            },
            boot_id(),
        )
        .unwrap();
    daemon.emit(ProcessEventKind::ProcessCreated, process.clone());
    (root, daemon, process)
}

#[tokio::test]
async fn delayed_exit_cannot_recreate_a_removed_process_or_reserve_its_name() {
    let (_root, daemon, mut process) = fixture();
    process.state = ProcessState::Exited;
    daemon.store.save_process(&process).unwrap();
    daemon.observe_process(process.clone());
    let response = process_route(
        State(daemon.clone()),
        AxumPath(process.id.clone()),
        Request::builder()
            .method(Method::DELETE)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let sequence = daemon.events.lock().unwrap().sequence;
    // Deliver a terminal notification after DELETE has already completed.
    daemon.observe_process(process.clone());
    assert!(
        !daemon
            .store
            .process_dir(&process.workspace_id, &process.id)
            .exists()
    );
    assert_eq!(daemon.events.lock().unwrap().sequence, sequence);
    let workspace = daemon.store.find_workspace("dev").unwrap();
    let replacement = daemon
        .store
        .create_process(
            &workspace,
            RunRequest {
                name: process.name.clone(),
                command: process.command.clone(),
                ..Default::default()
            },
            boot_id(),
        )
        .unwrap();
    assert_ne!(replacement.id, process.id);
}

#[test]
fn duplicate_terminal_and_older_execution_events_are_not_published() {
    let (_root, daemon, mut process) = fixture();
    process.state = ProcessState::Exited;
    daemon.store.save_process(&process).unwrap();
    daemon.observe_process(process.clone());
    let sequence = daemon.events.lock().unwrap().sequence;
    daemon.observe_process(process.clone());
    assert_eq!(daemon.events.lock().unwrap().sequence, sequence);
    let old_execution = process.clone();
    process.execution += 1;
    process.state = ProcessState::Starting;
    daemon.store.save_process(&process).unwrap();
    daemon.emit(ProcessEventKind::ProcessUpdated, process.clone());
    let sequence = daemon.events.lock().unwrap().sequence;
    daemon.observe_process(old_execution);
    assert_eq!(daemon.events.lock().unwrap().sequence, sequence);
    assert_eq!(
        daemon.store.find_process(&process.id).unwrap().execution,
        process.execution
    );
}

#[tokio::test]
async fn events_keep_their_payload_and_wait_requires_the_exit_notification() {
    let (_root, daemon, process) = fixture();
    daemon
        .group_event_bridges
        .lock()
        .unwrap()
        .insert(process.group_id.clone());
    // Disk may already contain a later state while notifications are queued.
    let mut exited = process.clone();
    exited.state = ProcessState::Exited;
    daemon.store.save_process(&exited).unwrap();
    let mut running = process.clone();
    running.state = ProcessState::Running;
    daemon.observe_process(running.clone());
    assert_eq!(
        daemon.events.lock().unwrap().processes[&process.id].state,
        ProcessState::Running
    );
    let waiting = daemon.wait_for_terminal(&process);
    tokio::pin!(waiting);
    assert!(futures_util::poll!(&mut waiting).is_pending());
    daemon.observe_process(exited);
    assert_eq!(waiting.await.unwrap().state, ProcessState::Exited);
    let sequence = daemon.events.lock().unwrap().sequence;
    daemon.observe_process(running);
    assert_eq!(daemon.events.lock().unwrap().sequence, sequence);
}

#[tokio::test]
async fn wait_recovers_terminal_metadata_without_a_group_bridge() {
    let (_root, daemon, mut process) = fixture();
    daemon
        .group_event_bridges
        .lock()
        .unwrap()
        .insert(process.group_id.clone());
    let original = process.clone();
    let waiting = daemon.wait_for_terminal(&original);
    tokio::pin!(waiting);
    assert!(futures_util::poll!(&mut waiting).is_pending());
    // The bridge can disappear after wait has subscribed, without a final event.
    daemon
        .group_event_bridges
        .lock()
        .unwrap()
        .remove(&process.group_id);
    process.state = ProcessState::Exited;
    daemon.store.save_process(&process).unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.state, ProcessState::Exited);
}
