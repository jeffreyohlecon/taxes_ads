# Null framing: precise nulls as identification of $g''$

The paper's contribution, if the empirics produce precise
nulls across many stacked tax events, is **not** "we ran a
sign test and found zero." It is: a sufficient-statistic
identification of the curvature of firms' advertising cost
functions, from reduced-form tax-induced demand shocks.

## Mechanics

Firm maximizes $(p - c - t) D(p, a) - g(a)$. Ad FOC:

$$(p - c - t) D_a(p, a) = g'(a)$$

$a^*$ moves with $t$ iff the RHS of this FOC moves with $t$.
Three channels can deliver a null on $da^*/dt$:

1. **No markup pass-through.** $(p - c - t)$ invariant to $t$.
   Testable separately via retail prices. If prices move,
   this isn't the story.

2. **Cross-partial offset.** Under non-separable demand
   ($D_{pA} \neq 0$), the persuasion and quantity channels
   can exactly cancel. Knife-edge for one category;
   implausible across many heterogeneous markets.

3. **Convex ad cost.** $g''$ large. Firm's optimal $a$
   barely responds to changes in marginal revenue from
   advertising.

Channels (1) and (2) can be ruled out empirically; channel
(3) is what remains. Precise nulls across SSB, liquor,
beer, tobacco $\Rightarrow$ effective $g''$ is large at
the DMA-month margin.

## Why this is a contribution (honest version)

Earlier framing claimed "rigid ad budgets" is a primitive
the IO/marketing literature uses. That was an oversell.
What's actually true:

- **Marketing science** documents sticky ad-budgeting
  qualitatively: "percent-of-sales" rule, annual upfronts,
  HQ top-down allocation. Stylized fact, not parameter.
- **Structural IO** (Dubois–Griffith–O'Connell, Sovinsky-
  Goeree, Doraszelski-Markovich) estimates a smooth $g(a)$
  jointly with many other parameters. Curvature is buried
  in the structural estimates, not a headline object.
- **Ad-stock literature** (Bronnenberg, Dubé-Hitsch-
  Manchanda, Hartmann-Klapper) emphasizes slow ad-effect
  accumulation. Related but distinct mechanism.

The contribution: **direct reduced-form identification of
$g''$ at the DMA-month margin**, without committing to a
full structural model. Marketing folklore gets a clean
number; IO models get a moment they can match.

## Reporting moves (5)

1. **CI in interpretable units, no stars.** Pooled stacked-
   event coefficient. "$0.01/oz excise raises retail price
   by X¢ but moves Spot TV ad spending by Y% (95% CI [a, b])."
   Inference via wild-cluster bootstrap or permutation.

2. **Bound what you rule out.** "We rule out ad-spending
   declines larger than Z% per percentage point of effective
   tax — an order of magnitude below the prediction of
   [baseline persuasive-demand model]."

3. **Map CI to $g''$.** Use the ad FOC and observed price
   pass-through (this paper + literature: Cawley-Frisvold-
   Hill on SSB, Conlon-Rao on alcohol). Report as
   $\partial \log a / \partial \log MR \leq \bar\eta$ with
   $\bar\eta$ small. Compare to curvature implied by
   Dubois et al. (2025) and other structural estimates.

4. **Pre-empt alternatives.**
   - Prices didn't move $\to$ show pass-through is non-zero.
   - Tax too small $\to$ split by tax magnitude.
   - Wrong frequency $\to$ run at quarterly/annual/multi-year.
   - Confounded comparison DMAs $\to$ permutation inference.
   - Wrong channel $\to$ acknowledge Spot TV is one channel;
     cite prior Philly-Baltimore radio/internet null as
     concordant.

5. **Heterogeneity table.** Coefficients by category with
   CIs. If all categories overlap zero with tight intervals,
   the rigidity claim is much stronger than from any single
   category. Visual: forest plot of by-category coefficients
   + CIs.

## Conclusion framing

Not "we find no effect." Rather:

> Across N stacked tax events, a precisely estimated null
> on advertising response, combined with non-zero price
> pass-through, implies an effective ad-cost convexity of
> at least $G^*$ at the DMA-month margin. Firms do not
> reoptimize Spot TV advertising at the frequency and
> granularity of local demand shocks of this magnitude.
> We interpret this as evidence of sticky ad-budget setting
> at the HQ/annual/upfront level — formalizing a stylized
> fact long observed in marketing science and pinning down
> a structural parameter typically buried inside larger IO
> models.

## What this displaces

- The earlier "sign test" framing (persuasive vs separable
  via $D_{pA}$) becomes a secondary cut, not the headline.
  Still valuable for cross-category heterogeneity if any
  category breaks the pattern.
- Matt's welfare/optimal-tax framework remains separate;
  cite from sb_incidence rather than import.
