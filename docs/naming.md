# Naming proposal: A×P×H

Status: proposal only. No package, binary, repository, or code identifiers have
been renamed yet.

## Name

The proposed product name is **A×P×H**, written as `axpxh` where identifiers
cannot contain the multiplication sign.

The name expands to:

> Agent × Process × Human

It describes the relationship at the center of the project: agents start and
control durable processes, while humans can inspect, attach to, and take over
those processes when needed. The multiplication signs express interaction
rather than a simple list of components.

## Pronunciation

The official pronunciation is:

> A cross P cross H

In Chinese it can be read as “A 叉 P 叉 H”. In informal conversation, `A-P-H`
may be used as a short form.

## Proposed naming system

| Context | Proposed form |
| --- | --- |
| Product and visual identity | `A×P×H` |
| Crate/package | `axpxh` |
| Repository | `axpxh` |
| Rust crate import | `axpxh` |
| CLI command | `axpxh` |
| Long-form tagline | `Agent × Process × Human` |

Using the same ASCII name for the package, repository, import, and command
avoids a split identity such as installing `axpxh` but invoking `amux`.

## Rationale

- It creates an independent identity that is not tied to a personal or
  organizational prefix.
- It reflects both agent automation and seamless human participation.
- The ASCII form is short and distinctive for package registries, search, and
  command-line use.
- The visual form makes the otherwise abstract ASCII spelling understandable.

The main tradeoff is first-read pronunciation. Documentation and visual assets
should therefore introduce the name consistently as:

```text
A×P×H
Agent × Process × Human
Pronounced “A cross P cross H”
```

## Before adoption

- Recheck availability across crates.io, Git hosting, and relevant package or
  domain registries immediately before publishing.
- Decide whether the existing `amux` CLI name needs a compatibility alias or a
  clean break.
- Define the migration plan for environment variables, state paths, daemon and
  shim names, documentation, and downstream integrations.
- Confirm capitalization and typography for logos and other visual assets.

