# Editor support

The DSRV language has editor support for Visual Studio Code, provided by two separate components:

- the **[dsrv-vscode](https://github.com/INTO-CPS-Association/dsrv-vscode)** extension, which supplies syntax highlighting, the `.dsrv` file association, and commands for running a model;
- the **[dsrv-lsp](https://github.com/INTO-CPS-Association/dsrv-lsp)** language server, which supplies completion and diagnostics over the Language Server Protocol.

The language server links the Trustworthiness Checker (TC) as a library and reuses its parser and type checker, so editor diagnostics reflect the same language definition the TC evaluates.

## What the extension provides

| Feature | Component | Notes |
|---|---|---|
| Syntax highlighting for `.dsrv` files | Extension | Static grammar; no binary required |
| Keyword and function completion | Language server | Not yet context-aware |
| Syntax and type diagnostics | Language server | Reported as editor squiggles |
| Run the current model | Extension | Invokes the `trustworthiness_checker` binary in a terminal |

Syntax highlighting works with the extension alone. Every other feature requires one or both binaries described below.

## Requirements

The extension does not bundle the binaries it drives. Two executables must be available before the language and run features work:

| Binary | Source repository | Purpose |
|---|---|---|
| `dsrv-lsp` | [dsrv-lsp](https://github.com/INTO-CPS-Association/dsrv-lsp) | Completion and diagnostics |
| `trustworthiness_checker` | This repository | Executing a model from the editor |

Build the checker as described in [Getting started](../getting-started.md). For a standalone binary rather than `cargo run`, build in release mode from the repository root:

```sh
cargo build --release --package trustworthiness_checker
```

The executable is written to `target/release/trustworthiness_checker`.

Build the language server the same way from a `dsrv-lsp` checkout.

The executable is written to `target/release/dsrv-lsp`.

## Install the extension

<!-- TODO -->

## Configure the binary paths

The extension contributes two settings, both available under **File -> Preferences -> Settings** by searching for `DSRV`:

| Setting | Meaning | Default |
|---|---|---|
| `DSRV.lspPath` | Path to the `dsrv-lsp` executable | Empty; `dsrv-lsp` is resolved from `PATH` |
| `DSRV.binaryPath` | Path to the `trustworthiness_checker` executable | `./target/release/trustworthiness_checker`, resolved against the workspace root |

Both settings accept an absolute path, a path relative to the workspace root, or a bare command name resolved from `PATH`.

The default value of `DSRV.binaryPath` assumes the open workspace is a checkout of this repository. When editing DSRV models in another project, set the setting to an absolute path or install the binary on `PATH`.

## Run a model from the editor

Open a `.dsrv` file. Run commands appear in the editor title bar as a drop-down menu, beside the run button:

| Command | Input file | Semantics |
|---|---|---|
| Run DSRV | `<name>.input` beside the model | `untimed` |
| Choose Input File and Run DSRV | Selected through a file dialog | `untimed` |
| Run DSRV with Typed semantics | `<name>.input` beside the model | `typed-untimed` |
| Choose Input file and run with Typed semantics | Selected through a file dialog | `typed-untimed` |

Each command opens a terminal and invokes the checker directly:

```text
<trustworthiness_checker> <model.dsrv> --input-file <input> \
    --language dsrv --semantics <semantics> --output-stdout
```

The simplest workflow keeps an input file beside the model with the same base name - `counter.dsrv` and `counter.input` - so **Run DSRV** needs no file dialog.

Note that the editor commands select `untimed` or `typed-untimed` explicitly, while the CLI default is `gradual-typed-untimed`. Running the same model from the editor and from a shell without options therefore selects different semantics. See the [CLI reference](../reference/cli.md) for the full set of semantics values.

## Troubleshooting

**No completion or diagnostics.** The language server did not start. Check the `DSRV` output channel (**View -> Output**, then select `DSRV`). A message of the form `Failed to start dsrv-lsp: spawn dsrv-lsp ENOENT` means the executable was not found: set `DSRV.lspPath` to its absolute path, or place `dsrv-lsp` on `PATH`.

**A run command reports that the input file was not found.** The extension looks for a file with the model's base name and the `.input` extension in the same folder. Create it, or use one of the "Choose input file" commands.

**A run command opens a terminal and the shell reports that the command does not exist.** `DSRV.binaryPath` does not point at a built checker. Build it as described above and set the setting to the resulting path.

**Diagnostics disagree with what the checker accepts.** The language server pins a specific revision of the checker and is updated deliberately, so a newly changed language definition may reach the checker before it reaches the langiage server. Rebuild `dsrv-lsp` against the checker revision you are running.