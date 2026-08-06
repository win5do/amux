use std::{
    io::{IsTerminal, Write},
    time::Duration,
};

use amux::{client::Client, config::Config, model::*, protocol::Response};
use base64::Engine;
use clap::{Args, Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use http::Method;
use http_body_util::BodyExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser)]
#[command(
    name = "amux",
    about = "Persistent process and TTY runtime for AI agents"
)]
struct Cli {
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Workspace {
        #[command(subcommand)]
        command: WorkspaceCommand,
    },
    Run(RunArgs),
    Ps {
        #[arg(short, long)]
        workspace: Option<String>,
    },
    Inspect {
        target: String,
    },
    Logs {
        target: String,
        #[arg(short, long)]
        follow: bool,
        #[arg(long, default_value_t = 0)]
        after: u64,
        #[arg(long)]
        jsonl: bool,
    },
    Attach {
        target: String,
        #[arg(long)]
        readonly: bool,
        #[arg(long)]
        takeover: bool,
        #[arg(long, default_value_t = 8192)]
        replay_bytes: usize,
        #[arg(long, default_value = "ctrl-p,ctrl-q")]
        detach_keys: String,
    },
    Signal {
        target: String,
        signal: String,
    },
    Stop {
        target: String,
        #[arg(short = 't', long = "time", default_value = "10s", value_parser = parse_duration)]
        grace: Duration,
    },
    Kill {
        target: String,
    },
    Wait {
        target: String,
    },
    Rm {
        target: String,
    },
    Restart {
        target: String,
    },
    Shell {
        #[arg(short, long)]
        workspace: String,
        #[arg(long, default_value = "shell")]
        name: String,
        #[arg(long)]
        shell: Option<String>,
    },
}

#[derive(Subcommand)]
enum WorkspaceCommand {
    Create {
        name: String,
        #[arg(long, default_value = "host")]
        runtime: String,
        #[arg(long, default_value = "")]
        root_dir: String,
        #[arg(short, long, value_parser = parse_env)]
        env: Vec<(String, String)>,
    },
    Ls,
    Inspect {
        target: String,
    },
    Rm {
        target: String,
    },
}

#[derive(Args)]
struct RunArgs {
    #[arg(short, long)]
    workspace: String,
    #[arg(long)]
    name: String,
    #[arg(long, default_value = "")]
    cwd: String,
    #[arg(long, default_value = "")]
    runtime: String,
    #[arg(short = 't', long)]
    tty: bool,
    #[arg(long, default_value_t = 40)]
    rows: u16,
    #[arg(long, default_value_t = 120)]
    cols: u16,
    #[arg(short, long, value_parser = parse_env)]
    env: Vec<(String, String)>,
    #[arg(required = true, trailing_var_arg = true)]
    command: Vec<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let json = cli.json;
    if let Err(error) = run(cli).await {
        if json {
            let response = match error.downcast_ref::<amux::protocol::ApiError>() {
                Some(value) => Response {
                    ok: false,
                    error: Some(value.clone()),
                    ..Response::default()
                },
                None => Response::fail("internal_error", error.to_string()),
            };
            println!("{}", serde_json::to_string(&response).unwrap());
        } else {
            eprintln!("{error}");
        }
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let client = Client::new(Config::load()?);
    client.ensure_daemon().await?;
    match cli.command {
        Command::Workspace { command } => match command {
            WorkspaceCommand::Create {
                name,
                runtime,
                root_dir,
                env,
            } => output(
                &client
                    .do_json(
                        Method::POST,
                        "/v1/workspaces",
                        Some(&CreateWorkspaceRequest {
                            name,
                            runtime,
                            root_dir,
                            env: env.into_iter().collect(),
                        }),
                    )
                    .await?,
                cli.json,
            )?,
            WorkspaceCommand::Ls => output(
                &client.do_empty(Method::GET, "/v1/workspaces").await?,
                cli.json,
            )?,
            WorkspaceCommand::Inspect { target } => output(
                &client
                    .do_empty(Method::GET, &format!("/v1/workspaces/{}", escape(&target)))
                    .await?,
                cli.json,
            )?,
            WorkspaceCommand::Rm { target } => output(
                &client
                    .do_empty(
                        Method::DELETE,
                        &format!("/v1/workspaces/{}", escape(&target)),
                    )
                    .await?,
                cli.json,
            )?,
        },
        Command::Run(args) => {
            let request = RunRequest {
                name: args.name,
                command: args.command,
                cwd: args.cwd,
                env: args.env.into_iter().collect(),
                io_mode: if args.tty { IoMode::Tty } else { IoMode::Pipe },
                runtime: args.runtime,
                initial_rows: args.rows,
                initial_cols: args.cols,
            };
            output(
                &client
                    .do_json(
                        Method::POST,
                        &format!("/v1/workspaces/{}/processes", escape(&args.workspace)),
                        Some(&request),
                    )
                    .await?,
                cli.json,
            )?;
        }
        Command::Ps { workspace } => {
            let query = workspace
                .map(|v| format!("?workspace={}", escape(&v)))
                .unwrap_or_default();
            output(
                &client
                    .do_empty(Method::GET, &format!("/v1/processes{query}"))
                    .await?,
                cli.json,
            )?;
        }
        Command::Inspect { target } => output(
            &client
                .do_empty(Method::GET, &process_path(&target, ""))
                .await?,
            cli.json,
        )?,
        Command::Logs {
            target,
            follow,
            after,
            jsonl,
        } => {
            if cli.json {
                anyhow::bail!("invalid_argument: logs is a stream; use --jsonl instead of --json")
            }
            stream_logs(
                client
                    .stream(&format!(
                        "{}?after={after}&follow={follow}",
                        process_path(&target, "logs")
                    ))
                    .await?,
                jsonl,
            )
            .await?;
        }
        Command::Signal { target, signal } => output(
            &client
                .do_json(
                    Method::POST,
                    &process_path(&target, "signal"),
                    Some(&SignalRequest { signal }),
                )
                .await?,
            cli.json,
        )?,
        Command::Stop { target, grace } => output(
            &client
                .do_json(
                    Method::POST,
                    &process_path(&target, "stop"),
                    Some(&StopRequest {
                        grace_millis: grace.as_millis() as i64,
                    }),
                )
                .await?,
            cli.json,
        )?,
        Command::Kill { target } => output(
            &client
                .do_json(
                    Method::POST,
                    &process_path(&target, "signal"),
                    Some(&SignalRequest {
                        signal: "KILL".into(),
                    }),
                )
                .await?,
            cli.json,
        )?,
        Command::Wait { target } => output(
            &client
                .do_empty(Method::GET, &process_path(&target, "wait"))
                .await?,
            cli.json,
        )?,
        Command::Rm { target } => output(
            &client
                .do_empty(Method::DELETE, &process_path(&target, ""))
                .await?,
            cli.json,
        )?,
        Command::Restart { target } => output(
            &client
                .do_empty(Method::POST, &process_path(&target, "restart"))
                .await?,
            cli.json,
        )?,
        Command::Shell {
            workspace,
            name,
            shell,
        } => {
            let shell = shell
                .or_else(|| std::env::var("SHELL").ok())
                .unwrap_or_else(|| "/bin/sh".into());
            let request = RunRequest {
                name,
                command: vec![shell],
                io_mode: IoMode::Tty,
                ..Default::default()
            };
            output(
                &client
                    .do_json(
                        Method::POST,
                        &format!("/v1/workspaces/{}/processes", escape(&workspace)),
                        Some(&request),
                    )
                    .await?,
                cli.json,
            )?;
        }
        Command::Attach {
            target,
            readonly,
            takeover,
            replay_bytes,
            detach_keys,
        } => {
            if let Ok(current) = std::env::var("AMUX_PROCESS_ID")
                && !current.trim().is_empty()
            {
                anyhow::bail!(
                    "nested_attach_not_allowed: cannot attach from inside amux process {current}; detach first"
                )
            }
            if readonly && takeover {
                anyhow::bail!("--readonly and --takeover cannot be used together")
            }
            let keys = parse_detach_keys(&detach_keys)?;
            let path = format!(
                "{}?readonly={readonly}&takeover={takeover}&replay_bytes={replay_bytes}",
                process_path(&target, "attach")
            );
            attach(client.attach(&path).await?, readonly, keys, &target).await?;
        }
    }
    Ok(())
}

fn output(bytes: &[u8], json: bool) -> anyhow::Result<()> {
    if json {
        std::io::stdout().write_all(bytes)?;
        return Ok(());
    }
    let response: Response = serde_json::from_slice(bytes)?;
    if !response.workspaces.is_empty() || response.workspace.is_some() {
        println!("NAME\tID\tRUNTIME\tSTATE\tCREATED");
        for value in response.workspaces.into_iter().chain(response.workspace) {
            println!(
                "{}\t{}\t{}\t{}\t{}",
                value.name,
                value.id,
                value.runtime,
                format!("{:?}", value.state).to_ascii_lowercase(),
                value
                    .created_at
                    .with_timezone(&chrono::Local)
                    .format("%Y-%m-%d %H:%M:%S")
            );
        }
    } else if !response.processes.is_empty() || response.process.is_some() {
        println!("WORKSPACE\tNAME\tID\tSTATE\tIO\tPID\tCOMMAND");
        for value in response.processes.into_iter().chain(response.process) {
            let p = value.process;
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                value.workspace_name,
                p.name,
                p.id,
                format!("{:?}", p.state).to_ascii_lowercase(),
                format!("{:?}", p.io_mode).to_ascii_lowercase(),
                if p.pid == 0 {
                    "-".into()
                } else {
                    p.pid.to_string()
                },
                p.command.join(" ")
            );
        }
    } else {
        println!("OK");
    }
    Ok(())
}

async fn stream_logs(mut body: hyper::body::Incoming, jsonl: bool) -> anyhow::Result<()> {
    let mut pending = Vec::new();
    while let Some(frame) = body.frame().await {
        let data = frame?.into_data().unwrap_or_default();
        pending.extend_from_slice(&data);
        while let Some(index) = pending.iter().position(|v| *v == b'\n') {
            let line = pending.drain(..=index).collect::<Vec<_>>();
            if jsonl {
                std::io::stdout().write_all(&line)?;
            } else if let Ok(event) = serde_json::from_slice::<OutputEvent>(&line)
                && let Some(data) = event.data
            {
                if data.encoding == "utf8" {
                    print!("{}", data.text);
                } else {
                    std::io::stdout().write_all(
                        &base64::engine::general_purpose::STANDARD.decode(data.base64)?,
                    )?;
                }
                std::io::stdout().flush()?;
            }
        }
    }
    Ok(())
}

async fn attach(
    socket: tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    readonly: bool,
    detach: [u8; 2],
    target: &str,
) -> anyhow::Result<()> {
    let _title = AttachTitle::set(target);
    let _raw = RawTerminal::enter()?;
    if std::io::stdin().is_terminal() {
        eprintln!("Attached to {target} (detach with ctrl-p,ctrl-q)");
    }
    let (mut sink, mut source) = socket.split();
    if !readonly {
        let (rows, cols) = terminal_size();
        sink.send(Message::Text(
            serde_json::to_string(&serde_json::json!({"type":"resize","rows":rows,"cols":cols}))?
                .into(),
        ))
        .await?;
    }
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut buffer = [0u8; 4096];
    let mut pending = false;
    let mut winch = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())?;
    loop {
        tokio::select! {
            read = stdin.read(&mut buffer), if !readonly => {
                let count = read?;
                if count == 0 { break; }
                let mut out = Vec::new();
                for byte in &buffer[..count] {
                    if pending {
                        if *byte == detach[1] { return Ok(()); }
                        out.push(detach[0]);
                        pending = false;
                    }

                    if *byte == detach[0] { pending = true; } else { out.push(*byte); }
                }
                if !out.is_empty() { sink.send(Message::Binary(out.into())).await?; }
            },
            message = source.next() => match message { Some(Ok(Message::Binary(data))) => { stdout.write_all(&data).await?; stdout.flush().await?; }, Some(Ok(Message::Text(text))) => { let value: serde_json::Value = serde_json::from_str(&text)?; if value["type"] == "exit" || value["type"] == "error" { break; } }, Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break, _ => {} },
            _ = winch.recv(), if !readonly => { let (rows, cols) = terminal_size(); sink.send(Message::Text(serde_json::to_string(&serde_json::json!({"type":"resize","rows":rows,"cols":cols}))?.into())).await?; }
        }
    }
    Ok(())
}

struct RawTerminal {
    original: Option<libc::termios>,
}

struct AttachTitle {
    active: bool,
}
impl AttachTitle {
    fn set(target: &str) -> Self {
        let active = std::io::stderr().is_terminal();
        if active {
            eprint!("\x1b[22;0t\x1b]0;amux: {}\x07", sanitize_title(target));
        }
        Self { active }
    }
}
impl Drop for AttachTitle {
    fn drop(&mut self) {
        if self.active {
            eprint!("\x1b[23;0t");
        }
    }
}
impl RawTerminal {
    fn enter() -> anyhow::Result<Self> {
        if !std::io::stdin().is_terminal() {
            return Ok(Self { original: None });
        }
        let fd = libc::STDIN_FILENO;
        let mut original = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(fd, &mut original) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut raw = original;
        unsafe {
            libc::cfmakeraw(&mut raw);
        }
        if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &raw) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Self {
            original: Some(original),
        })
    }
}
impl Drop for RawTerminal {
    fn drop(&mut self) {
        if let Some(original) = self.original {
            unsafe {
                libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &original);
            }
        }
    }
}

fn terminal_size() -> (u16, u16) {
    let mut size: libc::winsize = unsafe { std::mem::zeroed() };
    if unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut size) } == 0
        && size.ws_row > 0
        && size.ws_col > 0
    {
        (size.ws_row, size.ws_col)
    } else {
        (40, 120)
    }
}
fn sanitize_title(value: &str) -> String {
    value.chars().filter(|value| !value.is_control()).collect()
}
fn process_path(target: &str, action: &str) -> String {
    let target = target.split('/').map(escape).collect::<Vec<_>>().join("/");
    format!(
        "/v1/processes/{}{}",
        target,
        if action.is_empty() {
            String::new()
        } else {
            format!("/{action}")
        }
    )
}
fn escape(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
fn parse_env(value: &str) -> Result<(String, String), String> {
    value
        .split_once('=')
        .map(|(k, v)| (k.into(), v.into()))
        .ok_or_else(|| "environment must be KEY=VALUE".into())
}
fn parse_duration(value: &str) -> Result<Duration, String> {
    if let Some(v) = value.strip_suffix("ms") {
        return v
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| e.to_string());
    }
    if let Some(v) = value.strip_suffix('s') {
        return v
            .parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|e| e.to_string());
    }
    value
        .parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|e| e.to_string())
}
fn parse_detach_keys(value: &str) -> anyhow::Result<[u8; 2]> {
    let values = value
        .split(',')
        .map(|v| v.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    if values.len() != 2 {
        anyhow::bail!("detach keys must contain two comma-separated keys")
    }
    let mut out = [0; 2];
    for (i, value) in values.iter().enumerate() {
        out[i] = if value.starts_with("ctrl-") && value.len() == 6 {
            let b = value.as_bytes()[5];
            if b.is_ascii_lowercase() {
                b - b'a' + 1
            } else {
                anyhow::bail!("unsupported detach key {value}")
            }
        } else if value.len() == 1 {
            value.as_bytes()[0]
        } else {
            anyhow::bail!("unsupported detach key {value}")
        };
    }
    Ok(out)
}
