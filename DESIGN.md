---
name: Agent Durability Lab
description: Inspectable recovery evidence for fault-tested coding agents.
colors:
  surface-paper: "oklch(97.8% 0.008 78)"
  surface-raised: "oklch(99.2% 0.004 78)"
  surface-muted: "oklch(94.8% 0.012 80)"
  ink: "oklch(25% 0.025 252)"
  ink-soft: "oklch(45% 0.028 252)"
  rule: "oklch(84% 0.016 78)"
  accent: "oklch(48% 0.14 252)"
  accent-soft: "oklch(93% 0.035 252)"
  unsafe: "oklch(50% 0.15 34)"
  unsafe-soft: "oklch(94% 0.035 34)"
  protected: "oklch(46% 0.105 168)"
  protected-soft: "oklch(93% 0.035 168)"
  unfaulted: "oklch(48% 0.075 226)"
  focus: "oklch(58% 0.17 252)"
  code-surface: "oklch(24% 0.025 252)"
  code-ink: "oklch(94% 0.012 80)"
typography:
  display:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "3.2rem"
    fontWeight: 700
    lineHeight: 1.02
    letterSpacing: "-0.045em"
  headline:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "2rem"
    fontWeight: 700
    lineHeight: 1.12
    letterSpacing: "-0.025em"
  title:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.22rem"
    fontWeight: 700
    lineHeight: 1.25
  body:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: 1.58
  label:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.72rem"
    fontWeight: 780
    lineHeight: 1.2
    letterSpacing: "0.13em"
rounded:
  sm: "0.38rem"
  md: "0.55rem"
spacing:
  xs: "0.35rem"
  sm: "0.55rem"
  md: "0.8rem"
  lg: "1.25rem"
  xl: "1.5rem"
  section: "4rem"
components:
  button-primary:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.surface-raised}"
    typography: "{typography.body}"
    rounded: "{rounded.sm}"
    padding: "{spacing.sm} {spacing.md}"
  button-secondary:
    backgroundColor: "{colors.surface-paper}"
    textColor: "{colors.ink}"
    typography: "{typography.body}"
    rounded: "{rounded.sm}"
    padding: "{spacing.sm} {spacing.md}"
  tab-selected:
    backgroundColor: "{colors.surface-raised}"
    textColor: "{colors.ink}"
    typography: "{typography.body}"
    rounded: "{rounded.sm}"
    padding: "0.5rem 0.72rem"
  timeline-selected:
    backgroundColor: "{colors.accent-soft}"
    textColor: "{colors.ink}"
    typography: "{typography.body}"
    rounded: "{rounded.sm}"
    padding: "0.45rem {spacing.sm}"
---

# Design System: Agent Durability Lab

## Overview

**Creative North Star: "The Evidence Lab Notebook"**

This system feels like a well-run incident review laid out on warm technical paper: exact,
calm, and candid. It places the failed control, protected mechanism, and inspectable record in
one reading flow. Density is deliberate, but progressive disclosure keeps the first recovery
story legible before identities, authority, receipts, and provenance appear.

The interface is editorial rather than promotional. It rejects generic agent cookbook
galleries, polished success demos without negative controls, neon observability dashboards,
glass panels, decorative terminal motifs, and metric-card grids. Presentation must look
confident because the record is available, not because the surface looks futuristic.

**Key Characteristics:**

- Warm paper surfaces and dark ink establish a serious, daylight working environment.
- Unsafe, protected, and unfaulted colors annotate recorded state; they never replace text.
- Long rules, tables, and ordered timelines make comparisons explicit.
- One lifted inspector marks the current object of attention; the rest stays flat.
- The 78rem reading frame reflows at 62rem and 42rem without hiding evidence.

## Colors

Warm neutrals carry the page; blue is interactive, coral records unsafe outcomes, and green
records protected outcomes.

### Primary

- **Procedure Blue** (`accent`, `accent-soft`): links, selected timeline events, and Temporal
  procedure cues. Its rarity makes interaction obvious.

### Secondary

- **Unsafe Coral** (`unsafe`, `unsafe-soft`): distinguishing negative controls and the claim
  falsifier boundary.
- **Protected Green** (`protected`, `protected-soft`): admitted protected outcomes and verified
  replay receipts.
- **Unfaulted Blue** (`unfaulted`): calibration outcomes that must remain distinct from the
  protected mechanism.

### Neutral

- **Warm Evidence Paper** (`surface-paper`): the default working surface.
- **Raised Sheet** (`surface-raised`): the masthead, selected tabs, and the active event inspector.
- **Muted Ledger** (`surface-muted`): quiet controls and hover states.
- **Technical Ink** (`ink`, `ink-soft`): primary content and supporting explanation.
- **Ledger Rule** (`rule`): table, section, and record boundaries.
- **Native Record** (`code-surface`, `code-ink`): bounded raw JSON and native history.

### Named Rules

**The Evidence Color Rule.** Status color always appears with a variant name, verdict, count,
or authority label. Color alone never carries the conclusion.

**The One Interactive Hue Rule.** Procedure Blue is the only general interaction accent.
Coral and green belong to evidence state, never generic calls to action.

## Typography

**Display Font:** Inter with the system sans-serif fallback stack

**Body Font:** Inter with the system sans-serif fallback stack
**Label/Mono Font:** the platform UI monospace stack for identities, receipts, and raw records

**Character:** A single humanist sans voice keeps the product direct and operational. Tight
display spacing gives the page editorial authority; uppercase labels and monospace values make
record structure scannable without turning the whole interface into a terminal.

### Hierarchy

- **Display** (`display`): one restrained, short page title with a maximum measure of 13 characters.
- **Headline** (`headline`): scenario, comparison, evidence, responsibility, and falsifier sections.
- **Title** (`title`): timeline, identity, authority, effect, and inspector headings.
- **Body** (`body`): explanations and records, normally held near 65–72 characters per line.
- **Label** (`label`): eyebrow, field, lane, and table labels in uppercase.

### Named Rules

**The Record Voice Rule.** Prose uses the sans stack; only immutable identities, receipt IDs,
and source records use monospace.

## Elevation

The system is flat by default. Tonal separation and one-pixel ledger rules establish most
hierarchy. A single low ambient shadow is reserved for the active event inspector and a tiny
state shadow marks selected tabs; neither is decoration.

### Shadow Vocabulary

- **Active Record:** a diffuse low-contrast shadow under the event inspector, used only while it
  is the object paired with the selected timeline step.
- **Selected Control:** a one-pixel ambient shadow on the active episode or evidence tab.

### Named Rules

**The One Lifted Sheet Rule.** At most one content surface may look lifted in a viewport. If
multiple cards float, the system has become a dashboard and must be flattened.

## Components

### Buttons

- **Shape:** compact, gently rounded controls (`sm`).
- **Primary:** Technical Ink on Raised Sheet text; used only to load a freshly reverified record.
- **Hover / Focus:** hover preserves contrast; focus always uses the explicit three-pixel focus ring.
- **Secondary:** transparent evidence links with an ink border; opening a raw response remains
  visually subordinate to in-place inspection.

### Chips

- **Style:** episode and evidence tabs sit on Muted Ledger with transparent inactive states.
- **State:** the selected tab becomes a Raised Sheet with ink text and a quiet border. The label,
  not color, identifies the state; arrow keys move between episode tabs.

### Cards / Containers

- **Corner Style:** major sections have no rounded shell. The inspector and falsifier use square
  ledger boundaries; only controls use `sm` or `md` rounding.
- **Background:** the page remains Warm Evidence Paper. Raised Sheet is reserved for focus.
- **Shadow Strategy:** follow the One Lifted Sheet Rule.
- **Border:** one-pixel Ledger Rule except the coral falsifier boundary.
- **Internal Padding:** use `lg` or `xl`; full sections use `section` vertical rhythm.

### Navigation

The masthead is a thin Raised Sheet with a boxed ADL mark, quiet section links, and a protected
replay receipt. At narrow widths, links and the receipt leave the masthead rather than compress
the evidence below.

### Recovery Comparison

The unfaulted, unsafe, and protected rows always remain together and keep explicit verdict,
physical-effect, authority, and outcome columns. Narrow viewports use a named, keyboard-focusable
scroll region; columns are never silently removed.

### Recovery Timeline

Ordered event buttons connect with one-pixel rules and lane-colored dots. The selected step uses
Procedure Blue and drives one adjacent event inspector. Normalized events are navigation, not proof.

## Do's and Don'ts

### Do:

- **Do** show the unsafe control before explaining the protected mechanism.
- **Do** keep native history, raw evidence, source pins, and correction lineage one action away.
- **Do** preserve text labels beside every coral, green, or blue status mark.
- **Do** keep dense tables and JSON keyboard-focusable when they scroll.
- **Do** use daylight paper, ink, rules, and editorial spacing to support technical review.
- **Do** respect reduced motion and the visible focus ring on every interactive surface.

### Don't:

- **Don't** build a generic agent cookbook gallery optimized for breadth and copy-paste snippets.
- **Don't** present a polished Workflow demo that implies recovery correctness without a negative control.
- **Don't** use neon observability dashboards, glass panels, decorative terminal motifs, or metric-card grids.
- **Don't** hide provenance, correction lineage, or raw failure evidence behind a verdict.
- **Don't** claim exactly-once effects, provider compatibility, or performance without the required evidence.
- **Don't** turn evidence states into general brand colors or decorate sections with thick colored stripes.
