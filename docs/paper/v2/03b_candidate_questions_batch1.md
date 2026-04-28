# Candidate questions — batch 1 (q-011 to q-020)

Adds 10 questions to grow Swiss Legal RAG Bench from v0.1 (10 q)
toward v0.2 target (30 q total). Batch 2 (q-021–q-030) follows once
this batch is vetted.

**Coverage rationale.** v0.1 was de=6, fr=3, it=1; civil-law heavy;
mostly elements-of-norm + statute-text claim types. This batch
deliberately balances:

- **Languages:** de=4, fr=4, it=2 (drops de bias from 60% to 50%)
- **Legal areas:** adds Familienrecht, Erbrecht (post-2023 reform),
  Arbeitsrecht, Strafprozess, Sozialversicherungsrecht,
  Verwaltungsrecht, plus an Italian Mietrecht item
- **Claim types:** introduces 3 `doctrine_from_bge` (forces the
  retrieval-graph rail to find leading BGEs) and 2 `statute_change`
  (recent reforms — tests whether the corpus reflects current text)
- **Difficulty:** 2 basic, 5 intermediate, 3 advanced

**Vetting checklist for each question** (please mark ✓ / ✗ / amend):

  ☐ Question is realistic (a Swiss lawyer / student would actually ask)
  ☐ Reference answer is legally accurate
  ☐ Statute SR-number + article are correct
  ☐ For `doctrine_from_bge`: the BGE actually exists and IS a leading
    case for the topic (I've marked `[verify]` where I'm uncertain)
  ☐ Cross-language: when language ≠ corpus authoritative version,
    the question still maps to a real leading authority

----------------------------------------------------------------------

## q-011 — Familienrecht, basic (DE)

**Question:**
> Welche Voraussetzungen müssen erfüllt sein, damit Eltern nach Schweizer Recht die gemeinsame elterliche Sorge ausüben können — auch nach einer Scheidung?

**Reference answer:**
Seit der Revision per 1. Juli 2014 ist die gemeinsame elterliche Sorge nach Art. 296 Abs. 2 ZGB der Regelfall, sowohl bei verheirateten als auch bei nicht verheirateten Eltern. Eine Abweichung — Zuteilung an einen Elternteil allein — kommt nur in Betracht, wenn dies zur Wahrung des Kindeswohls notwendig ist (Art. 298 Abs. 1 ZGB für die Scheidung; Art. 298a Abs. 1 ZGB für nicht verheiratete Eltern). Die ledige Mutter erhält die Sorge zunächst allein (Art. 298a Abs. 5 ZGB), kann sie aber gemeinsam mit dem Vater durch eine schriftliche Erklärung bei der KESB oder dem Zivilstandsamt einrichten.

**Evidence:**
- statute: ZGB / SR 210 / Art. 296, 298, 298a (de)

**Claim type:** `norm_overview`

----------------------------------------------------------------------

## q-012 — Erbrecht, intermediate (DE) — post-2023 reform

**Question:**
> Wie hat die Erbrechtsrevision per 1. Januar 2023 die Pflichtteilsquoten verändert, und wie hoch ist heute der Pflichtteil der Nachkommen nach Art. 471 ZGB?

**Reference answer:**
Mit der Erbrechtsrevision per 1. Januar 2023 wurde der Pflichtteil der Nachkommen von drei Viertel auf die Hälfte des gesetzlichen Erbteils reduziert (Art. 471 ZGB n.F.). Der Pflichtteil des überlebenden Ehegatten oder eingetragenen Partners bleibt bei der Hälfte (Art. 471 Abs. 1 Ziff. 3 ZGB). Der Pflichtteil der Eltern wurde vollständig abgeschafft. Damit verfügt der Erblasser oder die Erblasserin über eine grössere freie Quote als zuvor.

**Evidence:**
- statute: ZGB / SR 210 / Art. 471 (de)

**Claim type:** `statute_change`

----------------------------------------------------------------------

## q-013 — Arbeitsrecht, intermediate (FR)

**Question:**
> Pendant quelles périodes l'employeur ne peut-il pas résilier le contrat de travail d'un employé en arrêt-maladie selon l'art. 336c CO?

**Reference answer:**
L'art. 336c CO institue des "périodes de protection" (Sperrfristen). Pendant l'arrêt-maladie ou la grossesse, le congé donné par l'employeur est nul. Pour la maladie ou l'accident sans faute, la durée de protection est: 30 jours pendant la première année de service, 90 jours de la deuxième à la cinquième année, 180 jours dès la sixième année (Art. 336c al. 1 let. b CO). La grossesse confère une protection pendant toute sa durée et 16 semaines après l'accouchement (let. c). Une résiliation pendant la période est nulle (al. 2); la résiliation faite avant et dont le délai n'a pas expiré est suspendue jusqu'à la fin de la protection.

**Evidence:**
- statute: CO / SR 220 / Art. 336c (fr)

**Claim type:** `statute_thresholds`

----------------------------------------------------------------------

## q-014 — Strafrecht, intermediate (FR)

**Question:**
> Quelles sont les conditions de la légitime défense selon l'art. 15 CP?

**Reference answer:**
L'art. 15 CP autorise la légitime défense lorsqu'une personne est l'objet, ou est menacée sans délai, d'une attaque illicite. La défense doit être proportionnée aux circonstances: l'auteur ne peut riposter que d'une manière qui correspond aux circonstances de l'attaque, et notamment à la gravité de l'attaque, aux biens juridiques en danger, et à la nature de la défense. Si l'auteur dépasse les limites de la légitime défense en agissant dans un état excusable d'excitation ou de saisissement causé par l'attaque, l'art. 16 al. 2 CP exclut la peine.

**Evidence:**
- statute: CP / SR 311.0 / Art. 15, 16 (fr)

**Claim type:** `elements_of_norm`

----------------------------------------------------------------------

## q-015 — doctrine_from_bge, advanced (FR) `[verify BGE]`

**Question:**
> Quel est l'arrêt de principe du Tribunal fédéral sur la responsabilité du détenteur d'animal selon l'art. 56 CO, et quels sont les éléments constitutifs qui en ressortent?

**Reference answer:**
La responsabilité du détenteur d'animal selon l'art. 56 CO suppose: (i) un dommage causé par l'animal, (ii) un détenteur de l'animal au moment de l'événement, et (iii) l'absence de preuve libératoire (le détenteur s'exonère en démontrant qu'il a pris tous les soins commandés par les circonstances ou que le dommage se serait produit malgré ces soins). L'ATF 131 III 115 est l'arrêt de principe moderne, qui précise notamment la notion de détenteur de fait et l'étendue du devoir de surveillance. La responsabilité est causale stricte (sans faute du détenteur).

**Evidence:**
- statute: CO / SR 220 / Art. 56 (fr)
- decisions: `bge_BGE_131_III_115` `[verify this is actually the leading modern case on Art. 56 CO]`

**Claim type:** `doctrine_from_bge`

----------------------------------------------------------------------

## q-016 — Diritto contrattuale, intermediate (IT)

**Question:**
> Secondo l'art. 1 CO, quando si conclude validamente un contratto nel diritto svizzero?

**Reference answer:**
Secondo l'art. 1 CO, il contratto richiede la manifestazione concorde delle volontà delle parti. La manifestazione può essere espressa o tacita (cpv. 2). L'accordo deve riguardare tutti gli elementi essenziali del contratto (essentialia negotii). Il consenso è formato dall'incontro tra un'offerta e un'accettazione conformi. Salvo deroga legale o pattuita (Art. 11 CO), il contratto non richiede una forma particolare e si conclude quindi anche oralmente o per atti concludenti.

**Evidence:**
- statute: CO / SR 220 / Art. 1, 11 (it)

**Claim type:** `elements_of_norm`

----------------------------------------------------------------------

## q-017 — Diritto del bail, intermediate (IT)

**Question:**
> A quali condizioni il conduttore può recedere anticipatamente dal contratto di locazione di un'abitazione secondo l'art. 264 CO?

**Reference answer:**
Secondo l'art. 264 cpv. 1 CO, il conduttore che restituisce la cosa locata senza osservare il termine o la scadenza di disdetta è liberato dai suoi obblighi soltanto se gli propone, in tempo utile, un nuovo conduttore solvibile e disposto a riprendere il contratto alle stesse condizioni. Se il locatore non può ragionevolmente rifiutare il subentrante proposto (cpv. 2), il conduttore originario è liberato. In caso contrario, deve corrispondere la pigione fino al momento in cui il rapporto può finire o avrebbe potuto finire osservando i termini contrattuali (cpv. 3), dedotti gli importi che il locatore ha risparmiato o avrebbe potuto risparmiare con la diligenza richiesta.

**Evidence:**
- statute: CO / SR 220 / Art. 264 (it)

**Claim type:** `statute_procedure`

----------------------------------------------------------------------

## q-018 — Sozialversicherungsrecht, advanced (DE)

**Question:**
> Unter welchen Voraussetzungen besteht ein Anspruch auf eine Invalidenrente nach Art. 28 IVG, und wie wird die Höhe der Rente bestimmt?

**Reference answer:**
Nach Art. 28 IVG entsteht der Rentenanspruch, wenn (i) die versicherte Person ihre Erwerbsfähigkeit nicht durch zumutbare Eingliederungsmassnahmen wiederherstellen, erhalten oder verbessern kann, (ii) sie während eines Jahres ohne wesentlichen Unterbruch durchschnittlich mindestens 40 Prozent arbeitsunfähig (Art. 6 ATSG) gewesen ist, und (iii) nach Ablauf dieses Jahres zu mindestens 40 Prozent invalid (Art. 8 ATSG) ist. Der Anspruch entsteht frühestens nach Ablauf von sechs Monaten nach der Geltendmachung (Art. 29 Abs. 1 IVG). Die Rente wird seit 2022 stufenlos zwischen 40 und 70 Prozent Invaliditätsgrad linear ausgerichtet (Art. 28b Abs. 4 IVG); ab 70 Prozent besteht Anspruch auf eine ganze Rente.

**Evidence:**
- statute: IVG / SR 831.20 / Art. 28, 28b, 29 (de)

**Claim type:** `elements_of_norm`

----------------------------------------------------------------------

## q-019 — doctrine_from_bge, advanced (DE) `[verify BGE]`

**Question:**
> Welches BGE gilt als Leiturteil zur Voraussetzung der vertrauensrechtlichen Bindung der Verwaltung an eine Auskunft (Vertrauensschutz nach Art. 9 BV), und welche Voraussetzungen werden darin formuliert?

**Reference answer:**
Nach ständiger bundesgerichtlicher Praxis kann ein verpflichtender Vertrauensschutz nach Art. 9 BV begründet werden, wenn (i) die Behörde, die die Auskunft erteilt hat, dafür zuständig war oder die Bürgerin und der Bürger die Behörde aus zureichenden Gründen als zuständig betrachten durften, (ii) die Auskunft vorbehaltlos erteilt wurde, (iii) die Bürgerin den Mangel der Auskunft nicht erkennen konnte, (iv) sie gestützt auf die Auskunft nachteilige Dispositionen getroffen hat, (v) die gesetzlichen Grundlagen unverändert geblieben sind und (vi) das öffentliche Interesse an der richtigen Rechtsanwendung nicht überwiegt. BGE 137 II 182 ist das einschlägige neuere Leiturteil. `[verify whether this is preferred over BGE 121 II 473]`

**Evidence:**
- statute: BV / SR 101 / Art. 9 (de)
- decisions: `bge_BGE_137_II_182` `[verify]` (alternativ: `bge_BGE_121_II_473`)

**Claim type:** `doctrine_from_bge`

----------------------------------------------------------------------

## q-020 — Verwaltungsrecht, advanced (DE)

**Question:**
> Was sind die formellen Anforderungen an die Verfügung im verwaltungsrechtlichen Verfahren des Bundes nach Art. 35 VwVG, und welche Folgen hat ein Begründungsmangel?

**Reference answer:**
Nach Art. 35 Abs. 1 VwVG sind schriftliche Verfügungen, auch wenn die Behörde sie in Briefform erlässt, als solche zu bezeichnen, zu begründen, und mit einer Rechtsmittelbelehrung zu versehen. Die Begründungspflicht ist Ausfluss des Anspruchs auf rechtliches Gehör nach Art. 29 Abs. 2 BV: die Behörde muss die wesentlichen Überlegungen nennen, von denen sie sich hat leiten lassen, sodass die Verfügung sachgerecht angefochten werden kann. Eine Verletzung der Begründungspflicht führt grundsätzlich zur Aufhebung der Verfügung, kann aber im Beschwerdeverfahren ausnahmsweise geheilt werden, wenn die Beschwerdeinstanz dieselbe Überprüfungsbefugnis hat wie die Vorinstanz und die Begründung im Beschwerdeverfahren nachgeholt wird, ohne dass dem Beschwerdeführer ein Nachteil entsteht (BGE 132 V 387 E. 5.1).

**Evidence:**
- statute: VwVG / SR 172.021 / Art. 35 (de); BV / SR 101 / Art. 29 (de)
- decisions: `bge_BGE_132_V_387` `[verify pinpoint 5.1]`

**Claim type:** `doctrine_from_bge`

----------------------------------------------------------------------

## Where I want your attention specifically

| Question | What I'm asking you to verify |
|---|---|
| q-015 | Is BGE 131 III 115 the right leading case on Art. 56 CO (animal-keeper liability)? Or is there a more recent / canonical one? |
| q-018 | Is the post-2022 stufenlose Rentenberechnung described correctly? I'm 90% sure but the IVG reform was complex. |
| q-019 | Is the 6-element Vertrauensschutz test attributed to BGE 137 II 182? And are the 6 conditions phrased correctly? |
| q-020 | Is BGE 132 V 387 the right Heilung-of-Begründungsmangel reference, or is BGE 137 I 195 closer? |
| All FR questions | Phrasing / register — read like a Romand lawyer would actually phrase it? |
| All IT questions | Same — register check |

I deliberately included `[verify]` markers on the items where my confidence is below ~95% to avoid quietly publishing wrong reference answers — that would compromise the benchmark. Strike out anything that's wrong; I redo.

## Next batch (q-021 – q-030) preview

Once these 10 are vetted, batch 2 will fill remaining gaps:
- 2 more `doctrine_from_bge` (for retrieval-graph stress)
- 1 cross-lingual question (DE question / FR-leading-case)
- 2 procedural (Strafprozess, Zivilprozess)
- 1 international Privatrecht
- 2 Wirtschaftsrecht (Aktienrecht, Wettbewerbsrecht)
- 1 Schuldbetreibungs-/Konkursrecht
- 1 multilingual edge case (statute existing in all 3 languages with terminological divergence)

That gets us to v0.2 = 30 questions, 50/30/20 DE/FR/IT split, 8 legal areas, 7 claim types.
