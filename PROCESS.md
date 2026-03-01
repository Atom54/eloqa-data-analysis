# PROCESS.md - Quanty's Data Workflow

## 🏗️ Architecture de travail

Deux modes selon le besoin :

### 1. Exploration (Marimo)
**Quand :** Analyse ponctuelle, investigation, hypothèses à tester
**Outil :** Marimo notebooks (`.py`)
**Dossier :** `notebooks/`

**Workflow :**
1. Créer un notebook Marimo dans `notebooks/`
2. Charger les données (CSV, API, Google Sheets...)
3. Explorer, visualiser, itérer
4. Exporter les findings en HTML ou PDF dans `exports/`
5. Partager le livrable à Thomas via Telegram

**Commande :**
```bash
source .venv/bin/activate
marimo edit notebooks/mon_analyse.py  # mode édition
marimo run notebooks/mon_analyse.py   # mode lecture
```

### 2. Dashboard consolidé (ClaudeCode)
**Quand :** Suivi régulier, métriques récurrentes, monitoring
**Outil :** ClaudeCode pour le développement
**Dossier :** `dashboard/`

**Workflow :**
1. Définir les métriques à suivre
2. Développer via ClaudeCode (demander à Axel si besoin d'aide archi)
3. Itérer et enrichir progressivement
4. Le dashboard grandit au fil du temps

**Stack dashboard :** À définir avec Thomas (React? Streamlit? Marimo app?)

---

## 📂 Structure du workspace

```
workspace-quanty/
├── PROCESS.md          ← ce fichier
├── SOUL.md             ← identité
├── USER.md             ← contexte Thomas
├── TOOLS.md            ← outils spécifiques
├── MEMORY.md           ← mémoire long-terme
├── memory/             ← logs quotidiens
├── notebooks/          ← explorations Marimo
├── dashboard/          ← dashboard consolidé
├── data/               ← datasets bruts
├── exports/            ← livrables (HTML, PDF)
├── scripts/            ← utilitaires
└── .venv/              ← environnement Python
```

---

## 🐍 Environnement Python

**Venv :** `.venv/` (Python 3.12)
**Activation :** `source .venv/bin/activate`
**Packages installés :**
- marimo 0.20.1
- pandas 3.0.1
- numpy, matplotlib, seaborn, plotly
- scipy, openpyxl, xlsxwriter, requests

**Ajouter un package :**
```bash
export PATH="$HOME/.local/bin:$PATH"
uv pip install --python .venv/bin/python <package>
```

---

## 📊 Métriques Eloqa clés (à suivre)

- **MRR** (Monthly Recurring Revenue)
- **Trial Start Rate** (priorité Thomas — onboarding/AHA moment)
- **Purchase Rate** (priorité Thomas — paywall/value perception)
- **Churn Rate**
- **LTV** (Lifetime Value)
- **CAC** (Customer Acquisition Cost)
- **Funnel de conversion** (Landing → Trial → Purchase)
- **Rétention** (D1, D7, D30)

---

## 🔌 Sources de données (status)

| Source | Status | Notes |
|--------|--------|-------|
| Google Sheets financier | ❌ En attente | Demandé le 02/02 |
| App Store Connect | ❌ Pas d'accès | |
| RevenueCat | ❌ Pas d'accès | |
| Firebase/Mixpanel | ❌ Pas d'accès | |
| Landing page analytics | ✅ CSV ponctuel | 317 sessions déc 2025 |

---

## ✅ Checklist nouvelle analyse

- [ ] Définir la question business
- [ ] Identifier la source de données
- [ ] Créer le notebook dans `notebooks/`
- [ ] Analyser et visualiser
- [ ] Documenter les findings
- [ ] Exporter le livrable dans `exports/`
- [ ] Logger dans `memory/YYYY-MM-DD.md`
- [ ] Envoyer à Thomas sur Telegram

---

## 🚀 Prochaines étapes

1. Thomas connecte les sources de données manquantes
2. Premier notebook d'exploration sur les nouvelles données
3. Définir le scope V1 du dashboard consolidé
4. Choisir la stack dashboard avec Thomas
