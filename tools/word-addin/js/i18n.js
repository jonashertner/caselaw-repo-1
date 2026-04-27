/**
 * OpenCaseLaw Word Add-in — Internationalization + Court Name Mapping
 * All UI strings in DE/FR/IT/EN, plus human-readable court names for 100+ courts.
 */

// Canton abbreviation → name per language
var CANTON_NAMES = {
  AG: { de: 'Aargau', fr: 'Argovie', it: 'Argovia', en: 'Aargau' },
  AI: { de: 'Appenzell I.Rh.', fr: 'Appenzell Rh.-Int.', it: 'Appenzello Int.', en: 'Appenzell I.Rh.' },
  AR: { de: 'Appenzell A.Rh.', fr: 'Appenzell Rh.-Ext.', it: 'Appenzello Est.', en: 'Appenzell A.Rh.' },
  BE: { de: 'Bern', fr: 'Berne', it: 'Berna', en: 'Bern' },
  BL: { de: 'Basel-Landschaft', fr: 'Bâle-Campagne', it: 'Basilea Campagna', en: 'Basel-Landschaft' },
  BS: { de: 'Basel-Stadt', fr: 'Bâle-Ville', it: 'Basilea Città', en: 'Basel-Stadt' },
  FR: { de: 'Freiburg', fr: 'Fribourg', it: 'Friburgo', en: 'Fribourg' },
  GE: { de: 'Genf', fr: 'Genève', it: 'Ginevra', en: 'Geneva' },
  GL: { de: 'Glarus', fr: 'Glaris', it: 'Glarona', en: 'Glarus' },
  GR: { de: 'Graubünden', fr: 'Grisons', it: 'Grigioni', en: 'Graubünden' },
  JU: { de: 'Jura', fr: 'Jura', it: 'Giura', en: 'Jura' },
  LU: { de: 'Luzern', fr: 'Lucerne', it: 'Lucerna', en: 'Lucerne' },
  NE: { de: 'Neuenburg', fr: 'Neuchâtel', it: 'Neuchâtel', en: 'Neuchâtel' },
  NW: { de: 'Nidwalden', fr: 'Nidwald', it: 'Nidvaldo', en: 'Nidwalden' },
  OW: { de: 'Obwalden', fr: 'Obwald', it: 'Obvaldo', en: 'Obwalden' },
  SG: { de: 'St. Gallen', fr: 'Saint-Gall', it: 'San Gallo', en: 'St. Gallen' },
  SH: { de: 'Schaffhausen', fr: 'Schaffhouse', it: 'Sciaffusa', en: 'Schaffhausen' },
  SO: { de: 'Solothurn', fr: 'Soleure', it: 'Soletta', en: 'Solothurn' },
  SZ: { de: 'Schwyz', fr: 'Schwyz', it: 'Svitto', en: 'Schwyz' },
  TG: { de: 'Thurgau', fr: 'Thurgovie', it: 'Turgovia', en: 'Thurgau' },
  TI: { de: 'Tessin', fr: 'Tessin', it: 'Ticino', en: 'Ticino' },
  UR: { de: 'Uri', fr: 'Uri', it: 'Uri', en: 'Uri' },
  VD: { de: 'Waadt', fr: 'Vaud', it: 'Vaud', en: 'Vaud' },
  VS: { de: 'Wallis', fr: 'Valais', it: 'Vallese', en: 'Valais' },
  ZG: { de: 'Zug', fr: 'Zoug', it: 'Zugo', en: 'Zug' },
  ZH: { de: 'Zürich', fr: 'Zurich', it: 'Zurigo', en: 'Zurich' },
  CH: { de: 'Bund', fr: 'Confédération', it: 'Confederazione', en: 'Federal' },
};

// Complete court code → human-readable name per language
var COURT_DISPLAY_NAMES = {
  // Federal courts
  bger:    { de: 'Bundesgericht', fr: 'Tribunal fédéral', it: 'Tribunale federale', en: 'Federal Supreme Court' },
  bge:     { de: 'BGE', fr: 'ATF', it: 'DTF', en: 'BGE' },
  bvger:   { de: 'Bundesverwaltungsgericht', fr: 'Tribunal administratif fédéral', it: 'Tribunale amministrativo federale', en: 'Federal Administrative Court' },
  bstger:  { de: 'Bundesstrafgericht', fr: 'Tribunal pénal fédéral', it: 'Tribunale penale federale', en: 'Federal Criminal Court' },
  bpatger: { de: 'Bundespatentgericht', fr: 'Tribunal fédéral des brevets', it: 'Tribunale federale dei brevetti', en: 'Federal Patent Court' },

  // Federal regulatory
  finma:   { de: 'FINMA', fr: 'FINMA', it: 'FINMA', en: 'FINMA' },
  finma_versicherungsrecht: { de: 'FINMA Versicherungsrecht', fr: 'FINMA Droit des assurances', it: 'FINMA Diritto assicurativo', en: 'FINMA Insurance Law' },
  weko:    { de: 'WEKO', fr: 'COMCO', it: 'COMCO', en: 'ComCo' },
  edoeb:   { de: 'EDÖB', fr: 'PFPDT', it: 'IFPDT', en: 'FDPIC' },
  ubi:     { de: 'UBI', fr: 'AIEP', it: 'AIRR', en: 'UBI' },
  elcom:   { de: 'ElCom', fr: 'ElCom', it: 'ElCom', en: 'ElCom' },
  postcom: { de: 'PostCom', fr: 'PostCom', it: 'PostCom', en: 'PostCom' },
  comcom:  { de: 'ComCom', fr: 'ComCom', it: 'ComCom', en: 'ComCom' },

  // Federal other
  ch_bundesrat: { de: 'Bundesrat', fr: 'Conseil fédéral', it: 'Consiglio federale', en: 'Federal Council' },
  ch_vb:   { de: 'Verwaltungspraxis des Bundes', fr: 'Jurisprudence des autorités administratives', it: 'Giurisprudenza delle autorità amministrative', en: 'Federal Administrative Practice' },
  bge_egmr: { de: 'EGMR (Schweiz)', fr: 'CEDH (Suisse)', it: 'CorteEDU (Svizzera)', en: 'ECHR (Switzerland)' },
  hudoc_ch: { de: 'EGMR (Schweiz)', fr: 'CEDH (Suisse)', it: 'CorteEDU (Svizzera)', en: 'ECHR (Switzerland)' },
  ta_sst:  { de: 'Schiedsgericht Sport', fr: 'Tribunal arbitral du sport', it: 'Tribunale arbitrale dello sport', en: 'Court of Arbitration for Sport' },
  emark:   { de: 'EMARK', fr: 'JICRA', it: 'GICRA', en: 'EMARK' },
  bge_historical: { de: 'BGE (historisch)', fr: 'ATF (historique)', it: 'DTF (storico)', en: 'BGE (historical)' },

  // Aargau
  ag_gerichte:    { de: 'Gerichte AG', fr: 'Tribunaux AG', it: 'Tribunali AG', en: 'Courts AG' },
  ag_verwaltungsgericht: { de: 'Verwaltungsgericht AG', fr: 'Tribunal administratif AG', it: 'Tribunale amministrativo AG', en: 'Administrative Court AG' },
  ag_versicherungsgericht: { de: 'Versicherungsgericht AG', fr: 'Tribunal des assurances AG', it: 'Tribunale delle assicurazioni AG', en: 'Insurance Court AG' },
  ag_handelsgericht: { de: 'Handelsgericht AG', fr: 'Tribunal de commerce AG', it: 'Tribunale di commercio AG', en: 'Commercial Court AG' },
  ag_strafgericht: { de: 'Strafgericht AG', fr: 'Tribunal pénal AG', it: 'Tribunale penale AG', en: 'Criminal Court AG' },
  ag_zivilgericht: { de: 'Zivilgericht AG', fr: 'Tribunal civil AG', it: 'Tribunale civile AG', en: 'Civil Court AG' },
  ag_spezialverwaltungsgericht: { de: 'Spezialverwaltungsgericht AG', fr: 'Tribunal administratif spécial AG', it: 'Tribunale amm. speciale AG', en: 'Special Administrative Court AG' },
  ag_regierungsrat: { de: 'Regierungsrat AG', fr: 'Conseil-exécutif AG', it: 'Consiglio di Stato AG', en: 'Government Council AG' },
  ag_anwaltskommission: { de: 'Anwaltskommission AG', fr: "Commission d'avocats AG", it: 'Commissione avvocati AG', en: 'Bar Commission AG' },
  ag_aufsichtskommission: { de: 'Aufsichtskommission AG', fr: 'Commission de surveillance AG', it: 'Commissione di vigilanza AG', en: 'Supervisory Commission AG' },
  ag_justizgericht: { de: 'Justizgericht AG', fr: 'Tribunal de la justice AG', it: 'Tribunale della giustizia AG', en: 'Justice Court AG' },
  ag_departement_bvu: { de: 'Departement BVU AG', fr: 'Département BVU AG', it: 'Dipartimento BVU AG', en: 'Department BVU AG' },
  ag_departement_vi: { de: 'Departement VI AG', fr: 'Département VI AG', it: 'Dipartimento VI AG', en: 'Department VI AG' },
  ag_departement_gs: { de: 'Departement GS AG', fr: 'Département GS AG', it: 'Dipartimento GS AG', en: 'Department GS AG' },
  ag_departement_bks: { de: 'Departement BKS AG', fr: 'Département BKS AG', it: 'Dipartimento BKS AG', en: 'Department BKS AG' },
  ag_baugesetzgebung: { de: 'Baugesetzgebung AG', fr: 'Législation des constructions AG', it: 'Legislazione edilizia AG', en: 'Building Legislation AG' },
  ag_weitere: { de: 'Weitere Behörden AG', fr: 'Autres autorités AG', it: 'Altre autorità AG', en: 'Other Authorities AG' },

  // Appenzell
  ai_gerichte: { de: 'Gerichte AI', fr: 'Tribunaux AI', it: 'Tribunali AI', en: 'Courts AI' },
  ar_gerichte: { de: 'Gerichte AR', fr: 'Tribunaux AR', it: 'Tribunali AR', en: 'Courts AR' },

  // Basel
  bl_gerichte: { de: 'Gerichte BL', fr: 'Tribunaux BL', it: 'Tribunali BL', en: 'Courts BL' },
  bs_appellationsgericht: { de: 'Appellationsgericht BS', fr: "Cour d'appel BS", it: "Corte d'appello BS", en: 'Court of Appeal BS' },
  bs_sozialversicherungsgericht: { de: 'Sozialversicherungsgericht BS', fr: 'Tribunal des assurances sociales BS', it: 'Tribunale delle assicurazioni sociali BS', en: 'Social Insurance Court BS' },
  bs_gerichte: { de: 'Gerichte BS', fr: 'Tribunaux BS', it: 'Tribunali BS', en: 'Courts BS' },

  // Bern
  be_verwaltungsgericht: { de: 'Verwaltungsgericht BE', fr: 'Tribunal administratif BE', it: 'Tribunale amministrativo BE', en: 'Administrative Court BE' },
  be_zivilstraf: { de: 'Zivil-/Strafgerichte BE', fr: 'Tribunaux civils/pénaux BE', it: 'Tribunali civili/penali BE', en: 'Civil/Criminal Courts BE' },
  be_bvd: { de: 'BVD BE', fr: 'Direction de la police BE', it: 'Direzione della polizia BE', en: 'Police Directorate BE' },
  be_steuerrekurs: { de: 'Steuerrekurskommission BE', fr: 'Commission de recours fiscal BE', it: 'Commissione di ricorso fiscale BE', en: 'Tax Appeal Commission BE' },
  be_weitere: { de: 'Weitere Behörden BE', fr: 'Autres autorités BE', it: 'Altre autorità BE', en: 'Other Authorities BE' },
  be_anwaltsaufsicht: { de: 'Anwaltsaufsicht BE', fr: 'Surveillance des avocats BE', it: 'Vigilanza avvocati BE', en: 'Bar Supervision BE' },

  // Fribourg, Geneva, Glarus, Graubünden, Jura
  fr_gerichte: { de: 'Gerichte FR', fr: 'Tribunaux FR', it: 'Tribunali FR', en: 'Courts FR' },
  ge_gerichte: { de: 'Gerichte GE', fr: 'Tribunaux GE', it: 'Tribunali GE', en: 'Courts GE' },
  gl_gerichte: { de: 'Gerichte GL', fr: 'Tribunaux GL', it: 'Tribunali GL', en: 'Courts GL' },
  gr_gerichte: { de: 'Gerichte GR', fr: 'Tribunaux GR', it: 'Tribunali GR', en: 'Courts GR' },
  ju_gerichte: { de: 'Gerichte JU', fr: 'Tribunaux JU', it: 'Tribunali JU', en: 'Courts JU' },

  // Luzern, Neuenburg, Nidwalden, Obwalden
  lu_gerichte: { de: 'Gerichte LU', fr: 'Tribunaux LU', it: 'Tribunali LU', en: 'Courts LU' },
  ne_gerichte: { de: 'Gerichte NE', fr: 'Tribunaux NE', it: 'Tribunali NE', en: 'Courts NE' },
  nw_gerichte: { de: 'Gerichte NW', fr: 'Tribunaux NW', it: 'Tribunali NW', en: 'Courts NW' },
  ow_gerichte: { de: 'Gerichte OW', fr: 'Tribunaux OW', it: 'Tribunali OW', en: 'Courts OW' },

  // St. Gallen
  sg_gerichte: { de: 'Gerichte SG', fr: 'Tribunaux SG', it: 'Tribunali SG', en: 'Courts SG' },
  sg_verwaltungsgericht: { de: 'Verwaltungsgericht SG', fr: 'Tribunal administratif SG', it: 'Tribunale amministrativo SG', en: 'Administrative Court SG' },
  sg_versicherungsgericht: { de: 'Versicherungsgericht SG', fr: 'Tribunal des assurances SG', it: 'Tribunale delle assicurazioni SG', en: 'Insurance Court SG' },
  sg_verwaltungsrekurskommission: { de: 'Verwaltungsrekurskommission SG', fr: 'Commission de recours administratif SG', it: 'Commissione di ricorso amm. SG', en: 'Administrative Appeals Commission SG' },
  sg_kantonsgericht: { de: 'Kantonsgericht SG', fr: 'Tribunal cantonal SG', it: 'Tribunale cantonale SG', en: 'Cantonal Court SG' },
  sg_handelsgericht: { de: 'Handelsgericht SG', fr: 'Tribunal de commerce SG', it: 'Tribunale di commercio SG', en: 'Commercial Court SG' },
  sg_publikationen: { de: 'Publikationen SG', fr: 'Publications SG', it: 'Pubblicazioni SG', en: 'Publications SG' },

  // Schaffhausen
  sh_obergericht: { de: 'Obergericht SH', fr: 'Tribunal supérieur SH', it: 'Tribunale superiore SH', en: 'High Court SH' },
  sh_gerichte: { de: 'Gerichte SH', fr: 'Tribunaux SH', it: 'Tribunali SH', en: 'Courts SH' },

  // Solothurn, Schwyz
  so_gerichte: { de: 'Gerichte SO', fr: 'Tribunaux SO', it: 'Tribunali SO', en: 'Courts SO' },
  sz_gerichte: { de: 'Gerichte SZ', fr: 'Tribunaux SZ', it: 'Tribunali SZ', en: 'Courts SZ' },
  sz_verwaltungsgericht: { de: 'Verwaltungsgericht SZ', fr: 'Tribunal administratif SZ', it: 'Tribunale amministrativo SZ', en: 'Administrative Court SZ' },

  // Thurgau
  tg_obergericht: { de: 'Obergericht TG', fr: 'Tribunal supérieur TG', it: 'Tribunale superiore TG', en: 'High Court TG' },
  tg_gerichte: { de: 'Gerichte TG', fr: 'Tribunaux TG', it: 'Tribunali TG', en: 'Courts TG' },

  // Ticino, Uri
  ti_gerichte: { de: 'Gerichte TI', fr: 'Tribunaux TI', it: 'Tribunali TI', en: 'Courts TI' },
  ur_gerichte: { de: 'Gerichte UR', fr: 'Tribunaux UR', it: 'Tribunali UR', en: 'Courts UR' },

  // Vaud
  vd_gerichte: { de: 'Gerichte VD', fr: 'Tribunaux VD', it: 'Tribunali VD', en: 'Courts VD' },
  vd_findinfo: { de: 'Findinfo VD', fr: 'Findinfo VD', it: 'Findinfo VD', en: 'Findinfo VD' },
  vd_omni:     { de: 'Omni VD', fr: 'Omni VD', it: 'Omni VD', en: 'Omni VD' },

  // Valais
  vs_gerichte: { de: 'Gerichte VS', fr: 'Tribunaux VS', it: 'Tribunali VS', en: 'Courts VS' },

  // Zug
  zg_verwaltungsgericht: { de: 'Verwaltungsgericht ZG', fr: 'Tribunal administratif ZG', it: 'Tribunale amministrativo ZG', en: 'Administrative Court ZG' },
  zg_obergericht: { de: 'Obergericht ZG', fr: 'Tribunal supérieur ZG', it: 'Tribunale superiore ZG', en: 'High Court ZG' },

  // Zürich
  zh_obergericht: { de: 'Obergericht ZH', fr: 'Tribunal supérieur ZH', it: 'Tribunale superiore ZH', en: 'High Court ZH' },
  zh_verwaltungsgericht: { de: 'Verwaltungsgericht ZH', fr: 'Tribunal administratif ZH', it: 'Tribunale amministrativo ZH', en: 'Administrative Court ZH' },
  zh_sozialversicherungsgericht: { de: 'Sozialversicherungsgericht ZH', fr: 'Tribunal des assurances sociales ZH', it: 'Tribunale delle assicurazioni sociali ZH', en: 'Social Insurance Court ZH' },
  zh_handelsgericht: { de: 'Handelsgericht ZH', fr: 'Tribunal de commerce ZH', it: 'Tribunale di commercio ZH', en: 'Commercial Court ZH' },
  zh_kassationsgericht: { de: 'Kassationsgericht ZH', fr: 'Cour de cassation ZH', it: 'Corte di cassazione ZH', en: 'Court of Cassation ZH' },
  zh_baurekursgericht: { de: 'Baurekursgericht ZH', fr: 'Tribunal des recours en matière de construction ZH', it: 'Tribunale dei ricorsi edilizi ZH', en: 'Building Appeals Court ZH' },
  zh_steuerrekursgericht: { de: 'Steuerrekursgericht ZH', fr: 'Tribunal des recours fiscaux ZH', it: 'Tribunale dei ricorsi fiscali ZH', en: 'Tax Appeals Court ZH' },
  zh_gerichte: { de: 'Gerichte ZH', fr: 'Tribunaux ZH', it: 'Tribunali ZH', en: 'Courts ZH' },
  zh_arbeitsgericht: { de: 'Arbeitsgericht ZH', fr: "Tribunal du travail ZH", it: 'Tribunale del lavoro ZH', en: 'Labour Court ZH' },
  zh_mietgericht: { de: 'Mietgericht ZH', fr: 'Tribunal des baux ZH', it: 'Tribunale delle locazioni ZH', en: 'Tenancy Court ZH' },
  zh_bezirksgericht_zuerich: { de: 'Bezirksgericht Zürich', fr: 'Tribunal de district de Zurich', it: 'Tribunale distrettuale di Zurigo', en: 'District Court Zurich' },
  zh_bezirksgericht_winterthur: { de: 'Bezirksgericht Winterthur', fr: 'Tribunal de district de Winterthour', it: 'Tribunale distrettuale di Winterthur', en: 'District Court Winterthur' },
  zh_bezirksgericht_horgen: { de: 'Bezirksgericht Horgen', fr: 'Tribunal de district de Horgen', it: 'Tribunale distrettuale di Horgen', en: 'District Court Horgen' },
  zh_bezirksgericht_dietikon: { de: 'Bezirksgericht Dietikon', fr: 'Tribunal de district de Dietikon', it: 'Tribunale distrettuale di Dietikon', en: 'District Court Dietikon' },
  zh_bezirksgericht_hinwil: { de: 'Bezirksgericht Hinwil', fr: 'Tribunal de district de Hinwil', it: 'Tribunale distrettuale di Hinwil', en: 'District Court Hinwil' },
  zh_bezirksgericht_dielsdorf: { de: 'Bezirksgericht Dielsdorf', fr: 'Tribunal de district de Dielsdorf', it: 'Tribunale distrettuale di Dielsdorf', en: 'District Court Dielsdorf' },
  zh_bezirksgericht_buelach: { de: 'Bezirksgericht Bülach', fr: 'Tribunal de district de Bülach', it: 'Tribunale distrettuale di Bülach', en: 'District Court Bülach' },
  zh_bezirksgericht_uster: { de: 'Bezirksgericht Uster', fr: 'Tribunal de district de Uster', it: 'Tribunale distrettuale di Uster', en: 'District Court Uster' },
  zh_bezirksgericht_pfaeffikon: { de: 'Bezirksgericht Pfäffikon', fr: 'Tribunal de district de Pfäffikon', it: 'Tribunale distrettuale di Pfäffikon', en: 'District Court Pfäffikon' },
  zh_bezirksgericht_affoltern: { de: 'Bezirksgericht Affoltern', fr: "Tribunal de district d'Affoltern", it: 'Tribunale distrettuale di Affoltern', en: 'District Court Affoltern' },
  zh_bezirksgericht_meilen: { de: 'Bezirksgericht Meilen', fr: 'Tribunal de district de Meilen', it: 'Tribunale distrettuale di Meilen', en: 'District Court Meilen' },
  zh_bezirksgericht_andelfingen: { de: 'Bezirksgericht Andelfingen', fr: "Tribunal de district d'Andelfingen", it: 'Tribunale distrettuale di Andelfingen', en: 'District Court Andelfingen' },
};

// UI translations
var UI_STRINGS = {
  // Consent
  consent_title: {
    de: 'Willkommen bei OpenCaseLaw',
    fr: 'Bienvenue sur OpenCaseLaw',
    it: 'Benvenuti su OpenCaseLaw',
    en: 'Welcome to OpenCaseLaw',
  },
  consent_text: {
    de: 'Dieses Add-in erm\u00F6glicht den sofortigen Zugriff auf Schweizer Gerichtsentscheide und Gesetzesartikel. Suchanfragen werden an unseren Server (mcp.opencaselaw.ch) gesendet. Es werden keine Dokumentinhalte \u00FCbertragen, ausser Sie nutzen die Pro-Funktionen auf markierten Text.',
    fr: 'Ce compl\u00E9ment permet l\u2019acc\u00E8s instantan\u00E9 aux d\u00E9cisions de justice et articles de loi suisses. Les requ\u00EAtes sont envoy\u00E9es \u00E0 notre serveur (mcp.opencaselaw.ch). Aucun contenu du document n\u2019est transmis, sauf si vous utilisez les fonctions Pro sur du texte s\u00E9lectionn\u00E9.',
    it: 'Questo componente aggiuntivo consente l\u2019accesso istantaneo alle decisioni giudiziarie e agli articoli di legge svizzeri. Le richieste vengono inviate al nostro server (mcp.opencaselaw.ch). Nessun contenuto del documento viene trasmesso, salvo l\u2019uso delle funzioni Pro sul testo selezionato.',
    en: 'This add-in provides instant access to Swiss court decisions and statute articles. Queries are sent to our server (mcp.opencaselaw.ch). No document content is transmitted unless you use the Pro features on selected text.',
  },
  consent_terms: {
    de: 'Nutzungsbedingungen', fr: 'Conditions d\u2019utilisation', it: 'Condizioni d\u2019uso', en: 'Terms of use',
  },
  consent_privacy: {
    de: 'Datenschutz', fr: 'Confidentialit\u00E9', it: 'Privacy', en: 'Privacy policy',
  },
  consent_accept: {
    de: 'Akzeptieren und fortfahren', fr: 'Accepter et continuer', it: 'Accetta e continua', en: 'Accept and continue',
  },

  brand_sub: {
    de: 'Schweizer Rechtsprechung', fr: 'Jurisprudence suisse', it: 'Giurisprudenza svizzera', en: 'Swiss Case Law',
  },

  // Search
  search_placeholder: {
    de: 'Stichwort, BGE-Nr., Aktenzeichen, Art. 41 OR ...',
    fr: 'Mot-clé, n° ATF, n° de dossier, Art. 41 CO ...',
    it: 'Parola chiave, n. DTF, n. di fascicolo, Art. 41 CO ...',
    en: 'Keyword, BGE no., docket no., Art. 41 OR ...',
  },
  welcome_count: {
    de: '{n} Entscheide',
    fr: '{n} décisions',
    it: '{n} decisioni',
    en: '{n} decisions',
  },
  quick_try: {
    de: 'Beispiele:', fr: 'Exemples :', it: 'Esempi:', en: 'Try:',
  },
  quick_examples: {
    de: 'Mietkündigung|BGE 133 III 121|Art. 41 OR|Notwehr|4A_747/2012',
    fr: 'résiliation bail|ATF 133 III 121|Art. 41 CO|légitime défense|4A_747/2012',
    it: 'disdetta locazione|DTF 133 III 121|Art. 41 CO|legittima difesa|4A_747/2012',
    en: 'lease termination|BGE 133 III 121|Art. 41 OR|self-defence|4A_747/2012',
  },
  how_it_works: {
    de: 'So funktioniert es \u2192', fr: 'Comment \u00E7a marche \u2192', it: 'Come funziona \u2192', en: 'How it works \u2192',
  },
  // Citation style picker (settings)
  cite_style_title: {
    de: 'Zitationsstil', fr: 'Style de citation', it: 'Stile di citazione', en: 'Citation style',
  },
  cite_style_parenthesised: {
    de: 'Standard (mit Klammern)', fr: 'Standard (entre parenthèses)', it: 'Standard (tra parentesi)', en: 'Default (parenthesised)',
  },
  cite_style_footnote: {
    de: 'Fussnote (ohne Klammern)', fr: 'Note de bas de page (sans parenthèses)', it: 'Nota a piè di pagina (senza parentesi)', en: 'Footnote (no parentheses)',
  },
  cite_style_brief: {
    de: 'Kurz (ohne Datum)', fr: 'Bref (sans date)', it: 'Breve (senza data)', en: 'Brief (no date)',
  },
  cite_style_long: {
    de: 'Lang (Gericht ausgeschrieben)', fr: 'Long (tribunal en toutes lettres)', it: 'Lungo (tribunale per esteso)', en: 'Long (court spelled out)',
  },
  // Keyboard shortcuts overlay (triggered by '?')
  help_title: {
    de: 'Tastaturk\u00FCrzel', fr: 'Raccourcis clavier', it: 'Scorciatoie da tastiera', en: 'Keyboard shortcuts',
  },
  help_intro: {
    de: 'Schnellzugriffe f\u00FCr h\u00E4ufige Aktionen.',
    fr: 'Raccourcis pour les actions fr\u00E9quentes.',
    it: 'Scorciatoie per le azioni frequenti.',
    en: 'Quick access for frequent actions.',
  },
  help_focus_search: {
    de: 'Suchfeld fokussieren', fr: 'Focaliser la recherche', it: 'Mettere a fuoco la ricerca', en: 'Focus search',
  },
  help_back: {
    de: 'Zur\u00FCck / Hilfe schliessen', fr: 'Retour / fermer l\u2019aide', it: 'Indietro / chiudi aiuto', en: 'Back / close help',
  },
  help_toggle_help: {
    de: 'Hilfe ein-/ausblenden', fr: 'Afficher/masquer l\u2019aide', it: 'Mostra/nascondi aiuto', en: 'Toggle help',
  },
  help_insert_top: {
    de: 'Erstes Resultat einf\u00FCgen', fr: 'Ins\u00E9rer le premier r\u00E9sultat', it: 'Inserisci il primo risultato', en: 'Insert top result',
  },
  help_open_first: {
    de: 'Erstes Resultat \u00F6ffnen', fr: 'Ouvrir le premier r\u00E9sultat', it: 'Apri il primo risultato', en: 'Open first result',
  },
  help_navigate: {
    de: 'Zwischen Resultaten navigieren', fr: 'Naviguer entre les r\u00E9sultats', it: 'Naviga tra i risultati', en: 'Navigate results',
  },
  guide_title: {
    de: 'So funktioniert OpenCaseLaw', fr: 'Comment fonctionne OpenCaseLaw', it: 'Come funziona OpenCaseLaw', en: 'How OpenCaseLaw works',
  },
  guide_step1_title: {
    de: 'Nachschlagen', fr: 'Consulter', it: 'Consultare', en: 'Look up',
  },
  guide_step1_desc: {
    de: 'Geben Sie eine BGE-Nummer (z.B. BGE 133 III 121), ein Aktenzeichen (z.B. 4A_747/2012) oder einen Gesetzesartikel (z.B. Art. 41 OR) ein. Der Volltext erscheint sofort.',
    fr: 'Entrez un num\u00E9ro ATF (p. ex. ATF 133 III 121), un num\u00E9ro de dossier (p. ex. 4A_747/2012) ou un article de loi (p. ex. Art. 41 CO). Le texte int\u00E9gral appara\u00EEt imm\u00E9diatement.',
    it: 'Inserisci un numero DTF (es. DTF 133 III 121), un numero di fascicolo (es. 4A_747/2012) o un articolo di legge (es. Art. 41 CO). Il testo integrale appare immediatamente.',
    en: 'Enter a BGE number (e.g. BGE 133 III 121), a docket number (e.g. 4A_747/2012) or a statute article (e.g. Art. 41 OR). The full text appears instantly.',
  },
  guide_step2_title: {
    de: 'Zitieren', fr: 'Citer', it: 'Citare', en: 'Cite',
  },
  guide_step2_desc: {
    de: '\u00ABEinf\u00FCgen\u00BB setzt die korrekt formatierte Zitierung an Ihre Cursorposition \u2014 in der Sprache Ihrer Wahl.',
    fr: '\u00ABIns\u00E9rer\u00BB place la citation correctement format\u00E9e \u00E0 la position du curseur \u2014 dans la langue de votre choix.',
    it: '\u00ABInserisci\u00BB posiziona la citazione formattata correttamente alla posizione del cursore \u2014 nella lingua scelta.',
    en: '\u00ABInsert\u00BB places the correctly formatted citation at your cursor \u2014 in your chosen language.',
  },
  guide_step3_title: {
    de: 'Pr\u00FCfen', fr: 'V\u00E9rifier', it: 'Verificare', en: 'Verify',
  },
  guide_step3_desc: {
    de: 'Markieren Sie eine Passage mit Entscheidreferenz in Ihrem Dokument. Die drei Pro-Werkzeuge finden \u00E4hnliche Entscheide, pr\u00FCfen ob Ihre Referenz die Aussage tr\u00E4gt, oder suchen einen st\u00FCtzenden Entscheid.',
    fr: 'S\u00E9lectionnez un passage avec une r\u00E9f\u00E9rence dans votre document. Les trois outils Pro trouvent des d\u00E9cisions similaires, v\u00E9rifient si votre r\u00E9f\u00E9rence soutient l\'affirmation, ou cherchent une d\u00E9cision \u00E0 l\'appui.',
    it: 'Seleziona un passaggio con un riferimento nel documento. I tre strumenti Pro trovano decisioni simili, verificano se il riferimento sostiene l\'affermazione, o cercano una decisione a supporto.',
    en: 'Select a passage with a decision reference in your document. The three Pro tools find similar decisions, verify whether your reference supports the claim, or search for a supporting decision.',
  },
  guide_coverage_title: {
    de: 'Abdeckung', fr: 'Couverture', it: 'Copertura', en: 'Coverage',
  },
  guide_coverage_desc: {
    de: 'BGer, BVGer, BStGer, BPatGer, FINMA, WEKO, ED\u00D6B, alle 26 Kantone. 965\u2009000+ Entscheide von 1875 bis heute. T\u00E4glich aktualisiert.',
    fr: 'TF, TAF, TPF, TFB, FINMA, COMCO, PFPDT, 26 cantons. 965\u2009000+ d\u00E9cisions de 1875 \u00E0 aujourd\'hui. Mis \u00E0 jour quotidiennement.',
    it: 'TF, TAF, TPF, TFB, FINMA, COMCO, IFPDT, 26 cantoni. 965\u2009000+ decisioni dal 1875 ad oggi. Aggiornato quotidianamente.',
    en: 'BGer, BVGer, BStGer, BPatGer, FINMA, ComCo, FDPIC, all 26 cantons. 965,000+ decisions from 1875 to today. Updated daily.',
  },
  guide_start: {
    de: 'Jetzt loslegen', fr: 'Commencer', it: 'Inizia ora', en: 'Get started',
  },
  results_count: {
    de: '{n} Entscheide gefunden',
    fr: '{n} décisions trouvées',
    it: '{n} decisioni trovate',
    en: '{n} decisions found',
  },
  lookup_not_found: {
    de: 'Entscheid oder Artikel nicht gefunden.\nPr\u00FCfen Sie die Eingabe \u2014 z.B. BGE 133 III 121 oder Art. 41 OR.',
    fr: 'D\u00E9cision ou article introuvable.\nV\u00E9rifiez la saisie \u2014 p. ex. ATF 133 III 121 ou Art. 41 CO.',
    it: 'Decisione o articolo non trovato.\nVerificare l\'inserimento \u2014 es. DTF 133 III 121 o Art. 41 CO.',
    en: 'Decision or article not found.\nCheck your input \u2014 e.g. BGE 133 III 121 or Art. 41 OR.',
  },
  no_results: {
    de: 'Keine Treffer gefunden.',
    fr: 'Aucun résultat trouvé.',
    it: 'Nessun risultato trovato.',
    en: 'No results found.',
  },
  no_results_hint: {
    de: 'Versuchen Sie einen allgemeineren Suchbegriff.',
    fr: 'Essayez un terme de recherche plus général.',
    it: 'Provare con un termine di ricerca più generale.',
    en: 'Try a more general search term.',
  },
  btn_insert: {
    de: 'Einfügen',
    fr: 'Insérer',
    it: 'Inserisci',
    en: 'Insert',
  },
  btn_fulltext: {
    de: 'Volltext',
    fr: 'Texte intégral',
    it: 'Testo integrale',
    en: 'Full text',
  },

  // Badges
  badge_leading: {
    de: 'Leitentscheid',
    fr: 'Arrêt de principe',
    it: 'Decisione di principio',
    en: 'Leading case',
  },
  badge_citations: {
    de: '{n} Zit.',
    fr: '{n} cit.',
    it: '{n} cit.',
    en: '{n} cit.',
  },

  // Detail view
  back: {
    de: 'Zurück zur Suche',
    fr: 'Retour à la recherche',
    it: 'Torna alla ricerca',
    en: 'Back to search',
  },
  back_short: {
    de: 'Zurück',
    fr: 'Retour',
    it: 'Indietro',
    en: 'Back',
  },
  section_regeste: {
    de: 'Regeste',
    fr: 'Régeste',
    it: 'Regesto',
    en: 'Summary',
  },
  section_erwaegungen: {
    de: 'Erwägungen',
    fr: 'Considérants',
    it: 'Considerandi',
    en: 'Considerations',
  },
  section_sachverhalt: {
    de: 'Sachverhalt', fr: 'Faits', it: 'Fatti', en: 'Facts',
  },
  section_dispositiv: {
    de: 'Dispositiv', fr: 'Dispositif', it: 'Dispositivo', en: 'Holding',
  },
  section_fulltext: {
    de: 'Volltext', fr: 'Texte int\u00E9gral', it: 'Testo integrale', en: 'Full text',
  },
  fulltext_link: {
    de: 'Volltext auf opencaselaw.ch lesen',
    fr: 'Lire le texte int\u00E9gral sur opencaselaw.ch',
    it: 'Leggi il testo integrale su opencaselaw.ch',
    en: 'Read full text on opencaselaw.ch',
  },
  fulltext_continue: {
    de: 'Weiter auf opencaselaw.ch lesen\u2026',
    fr: 'Continuer la lecture sur opencaselaw.ch\u2026',
    it: 'Continua a leggere su opencaselaw.ch\u2026',
    en: 'Continue reading on opencaselaw.ch\u2026',
  },
  section_statutes: {
    de: 'Gesetzesartikel',
    fr: 'Articles de loi',
    it: 'Articoli di legge',
    en: 'Statutes',
  },
  citations_label: {
    de: 'Zitierungen',
    fr: 'Citations',
    it: 'Citazioni',
    en: 'Citations',
  },
  loading: {
    de: 'Wird geladen...',
    fr: 'Chargement...',
    it: 'Caricamento...',
    en: 'Loading...',
  },

  // Laws view

  // Verify view
  verify_title: {
    de: 'Referenzprüfung',
    fr: 'Vérification de référence',
    it: 'Verifica del riferimento',
    en: 'Reference verification',
  },
  verify_selected: {
    de: 'Markierter Text',
    fr: 'Texte sélectionné',
    it: 'Testo selezionato',
    en: 'Selected text',
  },
  verify_checking: {
    de: 'Referenz wird geprüft...',
    fr: 'Vérification de la référence...',
    it: 'Verifica del riferimento...',
    en: 'Verifying reference...',
  },
  verdict_supports: {
    de: 'Zutreffend',
    fr: 'Conforme',
    it: 'Conforme',
    en: 'Supported',
  },
  verdict_partial: {
    de: 'Teilweise zutreffend',
    fr: 'Partiellement conforme',
    it: 'Parzialmente conforme',
    en: 'Partially supported',
  },
  verdict_contradicts: {
    de: 'Nicht zutreffend',
    fr: 'Non conforme',
    it: 'Non conforme',
    en: 'Not supported',
  },
  relevant_ew: {
    de: 'Relevante Erwägung',
    fr: 'Considérant pertinent',
    it: 'Considerando pertinente',
    en: 'Relevant consideration',
  },
  btn_insert_comment: {
    de: 'Kommentar einfügen',
    fr: 'Insérer commentaire',
    it: 'Inserisci commento',
    en: 'Insert comment',
  },
  btn_insert_result: {
    de: 'Ergebnis einfügen',
    fr: 'Insérer résultat',
    it: 'Inserisci risultato',
    en: 'Insert result',
  },
  verify_footer_pro: {
    de: 'OpenCaseLaw Pro \u00B7 Claude Haiku',
    fr: 'OpenCaseLaw Pro \u00B7 Claude Haiku',
    it: 'OpenCaseLaw Pro \u00B7 Claude Haiku',
    en: 'OpenCaseLaw Pro \u00B7 Claude Haiku',
  },
  no_selection: {
    de: 'Bitte markieren Sie einen Textabschnitt mit einer Entscheidreferenz.',
    fr: 'Veuillez sélectionner un passage contenant une référence.',
    it: 'Selezionare un passaggio con un riferimento a una decisione.',
    en: 'Please select a text passage containing a decision reference.',
  },

  // Settings
  settings_title: {
    de: 'Einstellungen',
    fr: 'Paramètres',
    it: 'Impostazioni',
    en: 'Settings',
  },
  settings_gear_title: {
    de: 'Einstellungen',
    fr: 'Paramètres',
    it: 'Impostazioni',
    en: 'Settings',
  },

  // Privacy / anonymous usage signal (see docs/datenschutz/)
  priv_signal_title: {
    de: 'Anonyme Nutzungsstatistik',
    fr: 'Statistiques d\u2019utilisation anonymes',
    it: 'Statistiche di utilizzo anonime',
    en: 'Anonymous usage signal',
  },
  priv_signal_body: {
    de: 'Sendet einen monatlich rotierenden Installations-Hash (8 Hex-Zeichen, SHA-256 mit Monats-Salt) mit jeder Anfrage. Erlaubt uns, aktive Installationen pro Monat zu z\u00e4hlen \u2014 ohne Sie wiedererkennen zu k\u00f6nnen.',
    fr: 'Envoie un hash d\u2019installation \u00e0 rotation mensuelle (8 caract\u00e8res hex, SHA-256 avec sel mensuel) \u00e0 chaque requ\u00eate. Nous permet de compter les installations actives par mois \u2014 sans pouvoir vous reconna\u00eetre.',
    it: 'Invia un hash di installazione a rotazione mensile (8 caratteri hex, SHA-256 con sale mensile) a ogni richiesta. Ci permette di contare le installazioni attive al mese \u2014 senza poterla riconoscere.',
    en: 'Sends a monthly-rotating install hash (8 hex chars, SHA-256 with monthly salt) on every request. Lets us count active installs per month \u2014 without being able to recognise you.',
  },
  priv_signal_note: {
    de: 'Deaktivieren Sie das H\u00e4kchen, um den Header vollst\u00e4ndig abzuschalten. Das Add-in funktioniert danach unver\u00e4ndert.',
    fr: 'D\u00e9cochez la case pour d\u00e9sactiver compl\u00e8tement cet en-t\u00eate. L\u2019add-in continue \u00e0 fonctionner normalement.',
    it: 'Deselezionate la casella per disattivare completamente l\u2019header. L\u2019add-in continua a funzionare normalmente.',
    en: 'Uncheck the box to disable the header completely. The add-in keeps working unchanged.',
  },
  priv_signal_more: {
    de: 'Mehr zum Datenschutz',
    fr: 'Plus sur la confidentialit\u00e9',
    it: 'Maggiori informazioni sulla privacy',
    en: 'More on privacy',
  },
  priv_signal_privacy_link: {
    de: 'Datenschutz',
    fr: 'Confidentialit\u00e9',
    it: 'Privacy',
    en: 'Privacy',
  },

  // Pro / Billing
  pro_feature_limit: {
    de: '25 Abfragen pro Tag',
    fr: '25 requ\u00EAtes par jour',
    it: '25 richieste al giorno',
    en: '25 queries per day',
  },
  pro_consent_text: {
    de: 'Ich akzeptiere die', fr: 'J\u2019accepte les', it: 'Accetto le', en: 'I accept the',
  },
  pro_consent_and: {
    de: 'und die', fr: 'et la', it: 'e la', en: 'and the',
  },
  pro_or_key: {
    de: 'oder Lizenzschlüssel eingeben',
    fr: 'ou entrer une clé de licence',
    it: 'o inserisci chiave di licenza',
    en: 'or enter license key',
  },
  pro_license_key: {
    de: 'Lizenzschlüssel',
    fr: 'Clé de licence',
    it: 'Chiave di licenza',
    en: 'License key',
  },
  btn_manage_sub: {
    de: 'Abo verwalten', fr: 'G\u00E9rer l\u2019abonnement', it: 'Gestisci abbonamento', en: 'Manage subscription',
  },
  btn_activate: {
    de: 'Aktivieren',
    fr: 'Activer',
    it: 'Attiva',
    en: 'Activate',
  },
  pro_active: {
    de: 'Aktiv',
    fr: 'Actif',
    it: 'Attivo',
    en: 'Active',
  },
  btn_remove_license: {
    de: 'Lizenz entfernen',
    fr: 'Supprimer la licence',
    it: 'Rimuovi licenza',
    en: 'Remove license',
  },
  pro_key_invalid: {
    de: 'Lizenzschlüssel ungültig oder abgelaufen. Bitte erneut eingeben.',
    fr: 'Clé de licence invalide ou expirée. Veuillez réessayer.',
    it: 'Chiave di licenza non valida o scaduta. Riprovare.',
    en: 'License key invalid or expired. Please try again.',
  },

  // Find related
  btn_find_related: {
    de: '\u00C4hnliche', fr: 'Similaires', it: 'Simili', en: 'Related',
  },
  btn_verify_pro: {
    de: 'Pr\u00FCfen', fr: 'V\u00E9rifier', it: 'Verificare', en: 'Verify',
  },
  source_link: {
    de: 'Originalquelle', fr: 'Source originale', it: 'Fonte originale', en: 'Original source',
  },
  btn_show_more: {
    de: 'Mehr anzeigen', fr: 'Afficher plus', it: 'Mostra di pi\u00F9', en: 'Show more',
  },
  btn_show_less: {
    de: 'Weniger anzeigen', fr: 'Afficher moins', it: 'Mostra meno', en: 'Show less',
  },
  btn_insert_ref: {
    de: 'Referenz einf\u00FCgen', fr: 'Ins\u00E9rer r\u00E9f\u00E9rence', it: 'Inserisci riferimento', en: 'Insert reference',
  },
  related_for: {
    de: 'Verwandte Entscheide f\u00FCr', fr: 'D\u00E9cisions li\u00E9es \u00E0', it: 'Decisioni correlate a', en: 'Related decisions for',
  },
  related_cited_by: {
    de: 'Zitiert von', fr: 'Cit\u00E9 par', it: 'Citato da', en: 'Cited by',
  },
  related_cites: {
    de: 'Zitiert', fr: 'Cite', it: 'Cita', en: 'Cites',
  },
  related_none: {
    de: 'Keine verwandten Entscheide im Zitationsnetz gefunden.',
    fr: 'Aucune d\u00E9cision li\u00E9e trouv\u00E9e dans le r\u00E9seau de citations.',
    it: 'Nessuna decisione correlata trovata nella rete di citazioni.',
    en: 'No related decisions found in the citation network.',
  },
  no_selection_related: {
    de: 'Bitte markieren Sie einen Textabschnitt mit einer Entscheidreferenz, um \u00E4hnliche Entscheide zu finden.',
    fr: 'Veuillez s\u00E9lectionner un passage contenant une r\u00E9f\u00E9rence pour trouver des d\u00E9cisions similaires.',
    it: 'Selezionare un passaggio con un riferimento per trovare decisioni simili.',
    en: 'Please select a text passage containing a decision reference to find related decisions.',
  },

  // Support (find-support Pro tool)
  tool_support: {
    de: 'Begr\u00FCndung', fr: 'Fondement', it: 'Fondamento', en: 'Support',
  },
  support_your_statement: {
    de: 'Ihre Aussage', fr: 'Votre affirmation', it: 'La vostra affermazione', en: 'Your statement',
  },
  support_searching: {
    de: 'Suche passende Entscheide...', fr: 'Recherche de d\u00E9cisions pertinentes...', it: 'Ricerca di decisioni pertinenti...', en: 'Finding relevant decisions...',
  },
  support_found: {
    de: '{n} st\u00FCtzende Entscheide gefunden', fr: '{n} d\u00E9cisions \u00E0 l\'appui trouv\u00E9es', it: '{n} decisioni a supporto trovate', en: '{n} supporting decisions found',
  },
  support_no_results: {
    de: 'Keine st\u00FCtzenden Entscheide gefunden. Versuchen Sie eine andere Formulierung.',
    fr: 'Aucune d\u00E9cision \u00E0 l\'appui trouv\u00E9e. Essayez une autre formulation.',
    it: 'Nessuna decisione a supporto trovata. Provare con un\'altra formulazione.',
    en: 'No supporting decisions found. Try a different formulation.',
  },
  no_selection_support: {
    de: 'Bitte markieren Sie eine Aussage in Ihrem Dokument, f\u00FCr die Sie eine Entscheidgrundlage suchen.',
    fr: 'Veuillez s\u00E9lectionner une affirmation dans votre document pour laquelle vous cherchez un fondement juridique.',
    it: 'Selezionare un\'affermazione nel documento per la quale cercare un fondamento giuridico.',
    en: 'Please select a statement in your document for which you need legal support.',
  },
  pro_feature_support: {
    de: 'St\u00FCtzende Entscheide finden (KI)', fr: 'Trouver des d\u00E9cisions \u00E0 l\'appui (IA)', it: 'Trovare decisioni a supporto (IA)', en: 'Find supporting decisions (AI)',
  },
  pro_feature_related: {
    de: 'Verwandte Entscheide im Zitationsnetz', fr: 'D\u00E9cisions connexes dans le r\u00E9seau de citations', it: 'Decisioni correlate nella rete di citazioni', en: 'Related decisions in citation network',
  },
  pro_feature_verify_new: {
    de: 'Referenzpr\u00FCfung mit KI', fr: 'V\u00E9rification des r\u00E9f\u00E9rences par IA', it: 'Verifica dei riferimenti con IA', en: 'AI reference verification',
  },

  // Errors
  error_daily_limit: {
    de: 'Tageslimit erreicht (25 pro Tag). Morgen stehen wieder alle Abfragen zur Verf\u00FCgung.',
    fr: 'Limite quotidienne atteinte (25 par jour). Toutes les requ\u00EAtes seront \u00E0 nouveau disponibles demain.',
    it: 'Limite giornaliero raggiunto (25 al giorno). Domani tutte le richieste saranno nuovamente disponibili.',
    en: 'Daily limit reached (25 per day). All queries will be available again tomorrow.',
  },
  error_rate_limit: {
    de: 'Zu viele Anfragen.',
    fr: 'Trop de requêtes.',
    it: 'Troppe richieste.',
    en: 'Too many requests.',
  },
  error_rate_wait: {
    de: 'Bitte {n}s warten.',
    fr: 'Veuillez patienter {n}s.',
    it: 'Attendere {n}s.',
    en: 'Please wait {n}s.',
  },
  error_connection: {
    de: 'Verbindungsfehler.',
    fr: 'Erreur de connexion.',
    it: 'Errore di connessione.',
    en: 'Connection error.',
  },
  btn_retry: {
    de: 'Erneut versuchen',
    fr: 'Réessayer',
    it: 'Riprova',
    en: 'Retry',
  },

  // Recent lookups
  recent_label: {
    de: 'Zuletzt:', fr: 'R\u00E9cents\u00A0:', it: 'Recenti:', en: 'Recent:',
  },

  // Scan

  // Audit (Pro) — full-document citation check via /api/attest
  audit_summary_ok: {
    de: '{n} Zitate · alle gültig',
    fr: '{n} citations · toutes valides',
    it: '{n} citazioni · tutte valide',
    en: '{n} citations · all valid',
  },
  audit_summary_mixed: {
    de: '{n} Zitate · {ok} ok · {bad} Probleme',
    fr: '{n} citations · {ok} ok · {bad} problèmes',
    it: '{n} citazioni · {ok} ok · {bad} problemi',
    en: '{n} citations · {ok} ok · {bad} issues',
  },
  audit_summary_none: {
    de: 'Keine Schweizer Entscheid-Zitate im Dokument gefunden.',
    fr: 'Aucune citation de décision suisse trouvée dans le document.',
    it: 'Nessuna citazione di decisione svizzera trovata nel documento.',
    en: 'No Swiss decision citations found in the document.',
  },
  audit_running: {
    de: 'Dokument wird geprüft...', fr: 'Vérification en cours...', it: 'Verifica in corso...', en: 'Auditing document...',
  },
  audit_issue_not_found: {
    de: 'Entscheid nicht in der Datenbank',
    fr: 'Décision absente de la base',
    it: 'Decisione non nel database',
    en: 'Decision not in database',
  },
  audit_issue_pinpoint: {
    de: 'Erwägung existiert nicht',
    fr: 'Considérant inexistant',
    it: 'Considerando inesistente',
    en: 'Pinpoint not found',
  },
  audit_issue_unknown: {
    de: 'Problem mit Zitat',
    fr: 'Problème de citation',
    it: 'Problema con citazione',
    en: 'Citation issue',
  },
  audit_valid_pinpoints: {
    de: 'Gültige Erwägungen:', fr: 'Considérants valides :', it: 'Considerandi validi:', en: 'Valid pinpoints:',
  },
  audit_jump_to: {
    de: 'Im Dokument zeigen', fr: 'Afficher dans le document', it: 'Mostra nel documento', en: 'Show in document',
  },
  audit_insert_comment: {
    de: 'Kommentar einfügen', fr: 'Insérer commentaire', it: 'Inserisci commento', en: 'Insert comment',
  },
  audit_comment_all: {
    de: 'Alle Probleme als Kommentare einfügen',
    fr: 'Insérer tous les problèmes en commentaires',
    it: 'Inserisci tutti i problemi come commenti',
    en: 'Insert all issues as comments',
  },
  audit_no_word: {
    de: 'Diese Funktion ist nur in Word verfügbar.',
    fr: 'Cette fonction n\'est disponible que dans Word.',
    it: 'Questa funzione è disponibile solo in Word.',
    en: 'This feature is only available inside Word.',
  },

  // Tab bar

  // Welcome cards (replace chip wall)

  // Audit tab entry state (no audit run yet)
  audit_passed_label: {
    de: '{n} gültige Zitate',
    fr: '{n} citations valides',
    it: '{n} citazioni valide',
    en: '{n} valid citations',
  },
  btn_insert_link: {
    de: 'Mit Hyperlink einfügen',
    fr: 'Insérer avec lien',
    it: 'Inserisci con collegamento',
    en: 'Insert with hyperlink',
  },

  // Pro hero (settings page)
  pro_hero_title: {
    de: 'Halluzinations-freie Schweizer Zitate',
    fr: 'Citations suisses sans hallucination',
    it: 'Citazioni svizzere senza allucinazioni',
    en: 'Hallucination-free Swiss citations',
  },
  pro_hero_sub: {
    de: 'Das ganze Dokument in einem Klick auf erfundene Entscheide und falsche Erwägungen geprüft.',
    fr: 'Tout votre document, en un clic, vérifié contre les décisions inventées et considérants erronés.',
    it: 'Tutto il documento, con un clic, verificato contro decisioni inventate e considerandi errati.',
    en: 'Your entire document, in one click, checked for fabricated decisions and wrong pinpoints.',
  },
  btn_upgrade_price: {
    de: 'Pro freischalten — CHF 5 / Monat',
    fr: 'Débloquer Pro — CHF 5 / mois',
    it: 'Sblocca Pro — CHF 5 / mese',
    en: 'Unlock Pro — CHF 5 / month',
  },
  pro_cancel_note: {
    de: 'Monatlich kündbar · Stripe-Abrechnung · MwSt. inkl.',
    fr: 'Résiliable chaque mois · Facturation Stripe · TVA incl.',
    it: 'Disdicibile ogni mese · Fatturazione Stripe · IVA incl.',
    en: 'Cancel anytime · Stripe billing · VAT incl.',
  },

  // Statute verbatim insert
  btn_insert_verbatim: {
    de: 'Wortlaut einfügen',
    fr: 'Insérer le texte',
    it: 'Inserisci il testo',
    en: 'Insert verbatim text',
  },

  // Bibliography insert
  audit_insert_bibliography: {
    de: 'Quellenverzeichnis einfügen',
    fr: 'Insérer la liste des sources',
    it: 'Inserisci elenco delle fonti',
    en: 'Insert bibliography',
  },
  bibliography_heading: {
    de: 'Zitierte Entscheide',
    fr: 'Décisions citées',
    it: 'Decisioni citate',
    en: 'Cited decisions',
  },

  // First-run hint
  hint_welcome: {
    de: 'Willkommen — Stichwort oder Referenz suchen. Mit dem Banner oben prüfen Sie das ganze Dokument in einem Klick.',
    fr: 'Bienvenue — recherchez un mot-clé ou une référence. Le bandeau en haut vérifie tout le document en un clic.',
    it: 'Benvenuto — cerca una parola chiave o un riferimento. Il banner in alto verifica tutto il documento con un clic.',
    en: 'Welcome — search a keyword or reference. The banner above audits your whole document in one click.',
  },
  hint_dismiss: {
    de: 'Verstanden', fr: 'Compris', it: 'Capito', en: 'Got it',
  },
  audit_start_button: {
    de: 'Dokument jetzt prüfen', fr: 'Vérifier le document', it: 'Verifica documento', en: 'Audit document now',
  },
  audit_secondary_title: {
    de: 'Weitere Werkzeuge',
    fr: 'Autres outils',
    it: 'Altri strumenti',
    en: 'More tools',
  },

  // Audit status strip (persistent banner above the search view)
  strip_idle: {
    de: 'Dokument auf Zitate prüfen',
    fr: 'Vérifier les citations du document',
    it: 'Verifica citazioni del documento',
    en: 'Check document citations',
  },
  strip_clean: {
    de: 'Alle {n} Zitate gültig',
    fr: '{n} citations valides',
    it: '{n} citazioni valide',
    en: 'All {n} citations valid',
  },
  strip_issues: {
    de: '{n} Zitate prüfen',
    fr: '{n} citations à revoir',
    it: '{n} citazioni da rivedere',
    en: '{n} citations to review',
  },
  strip_error: {
    de: 'Audit fehlgeschlagen — erneut versuchen',
    fr: 'Audit échoué — réessayer',
    it: 'Audit fallito — riprova',
    en: 'Audit failed — try again',
  },
  btn_audit_again: {
    de: 'Erneut prüfen',
    fr: 'Vérifier à nouveau',
    it: 'Verifica nuovamente',
    en: 'Audit again',
  },
  audit_comments_inserted: {
    de: '{n} Kommentare ins Dokument eingefügt — im Dokument prüfen.',
    fr: '{n} commentaires insérés dans le document — vérifier dans le document.',
    it: '{n} commenti inseriti nel documento — controlla nel documento.',
    en: '{n} comments inserted into your document — review in the document.',
  },
};

/**
 * Get a translated UI string. Supports {n} and {ref} placeholders.
 */
// Per-key dedup so missing-translation warnings don't spam the console
// (the same key may render dozens of times in a single view pass).
var _missingI18nKeysSeen = (typeof Object.create === 'function') ? Object.create(null) : {};

function t(key, lang, replacements) {
  var entry = UI_STRINGS[key];
  if (!entry) {
    if (!_missingI18nKeysSeen[key]) {
      _missingI18nKeysSeen[key] = 1;
      if (typeof console !== 'undefined' && console.warn) {
        console.warn('[i18n] missing key: ' + key + ' (rendered as the key name itself)');
      }
    }
    return key;
  }
  var str = entry[lang];
  if (str === undefined) {
    var fallbackKey = key + '/' + (lang || '');
    if (!_missingI18nKeysSeen[fallbackKey]) {
      _missingI18nKeysSeen[fallbackKey] = 1;
      if (typeof console !== 'undefined' && console.warn) {
        console.warn('[i18n] key "' + key + '" missing in language "' + lang +
                     '" — falling back to DE.');
      }
    }
    str = entry.de || key;
  }
  if (replacements) {
    Object.keys(replacements).forEach(function (k) {
      str = str.replace('{' + k + '}', replacements[k]);
    });
  }
  return str;
}

/**
 * Get human-readable court name for a court code.
 * Falls back to uppercase court code if not mapped.
 */
function getCourtName(courtCode, lang) {
  if (!courtCode) return '';
  lang = lang || 'de';
  var code = courtCode.toLowerCase();
  var entry = COURT_DISPLAY_NAMES[code];
  if (entry) return entry[lang] || entry.de;
  // Fallback: uppercase the code
  return courtCode.toUpperCase();
}

// Export for Node.js (tests)
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    COURT_DISPLAY_NAMES: COURT_DISPLAY_NAMES,
    CANTON_NAMES: CANTON_NAMES,
    UI_STRINGS: UI_STRINGS,
    t: t,
    getCourtName: getCourtName,
  };
}
