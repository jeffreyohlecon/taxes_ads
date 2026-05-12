# Related theory: Matt's sb_incidence section

Matt Brown (coauthor on taxes_ads) has a parallel theory
section in `sb_incidence/tax_theory/`, ~13 commits Feb 9 –
Mar 25 2026, integrated into `sb_incidence/paper/draft.tex`
Mar 20-25.

Files:
- [model.tex](../../sb_incidence/tax_theory/model.tex) — 596 lines, 7 rounds
- [section_av_tax.tex](../../sb_incidence/tax_theory/section_av_tax.tex) — 717 lines
- [paper_section.tex](../../sb_incidence/tax_theory/paper_section.tex) — 218 lines, paper version
- [model_notes.md](../../sb_incidence/tax_theory/model_notes.md), [feedback.md](../../sb_incidence/tax_theory/feedback.md)

## What it does

Monopolist chooses price + advertising. Consumers biased
with money-metric quality $\gamma(a) = \bar\gamma + \alpha a$.
Compares specific vs ad valorem under internalities.

Headline objects:
- Modified Pigouvian: $m - \gamma = \alpha Q/g''$
- $\alpha^*$ threshold for ad-valorem-vs-specific welfare ranking
- Tinbergen: $\tau^* = \alpha/(1-\alpha)$
- "Direct quality channel" $\alpha Q^2/g''$ — makes ad valorem
  cut ads even at $c \approx 0$

Antecedent: Pirttilä (2002). Concurrent: Dubois–Abi-Rafeh–
Griffith–O'Connell (2025).

## Scope mismatch with taxes_ads

1. **Wrong tax base.** Matt's headline is ad valorem with
   $c \approx 0$ (GGR/revenue taxes). Every tax in taxes_ads
   (SSB, liquor, beer, tobacco) is a per-unit excise with
   $c > 0$. The "direct quality channel" is a feature of the
   ad valorem *base*; an excise does not have it.

2. **No new sign for an excise.** Matt's framework, applied
   to an excise, collapses to Dorfman-Steiner: excise lowers
   $Q$, ad FOC lowers $a$. That is already the separable-demand
   branch of [paper/model.tex](model.tex). No new content for
   the empirical sign test.

3. **$\alpha$ is unobservable.** His framework loads on the
   bias share $\alpha$, which requires strong identifying
   assumptions. taxes_ads reads persuasive-vs-separable off
   the *sign* of $dA/dt$ without taking a stand on $\alpha$.

## Treatment

Cite Matt's sb_incidence theory as related coauthor work.
Do not import the welfare / optimal-tax machinery. The
behavioral-vs-separable balance is useful framing; $\alpha$-
identification is not this paper's lift.
