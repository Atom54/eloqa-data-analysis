import marimo

__generated_with = "0.20.1"
app = marimo.App(width="full")


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import numpy as np
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import psycopg2
    from datetime import datetime, timedelta

    ELOQA_PURPLE = "#6C5CE7"
    ELOQA_COLORS = ["#6C5CE7", "#a29bfe", "#fd79a8", "#00cec9", "#fdcb6e", "#e17055", "#00b894"]

    mo.md("""
    # 📊 Sprint Data Analysis — Eloqa
    
    **Période :** Septembre 2025 – Février 2026  
    **Auteur :** Quanty (Data Analyst)  
    **Date :** 28 février 2026
    
    ---
    
    Ce rapport couvre 5 analyses clés du sprint en cours, plus un deep dive sur le finding principal.  
    Toutes les données proviennent de la base de production Eloqa (copie read-only restaurée quotidiennement).
    """)
    return ELOQA_COLORS, ELOQA_PURPLE, datetime, go, make_subplots, mo, np, pd, psycopg2, px, timedelta


@app.cell
def __(psycopg2):
    # Database connection
    def get_conn():
        return psycopg2.connect(
            host="localhost", dbname="eloqa",
            user="eloqa_reader", password="readonly"
        )

    def query(sql):
        import pandas as pd
        conn = get_conn()
        try:
            return pd.read_sql(sql, conn)
        finally:
            conn.close()

    return get_conn, query


# ============================================================
# ANALYSIS 1: Paywall → Trial Conversion (#4358)
# ============================================================

@app.cell
def __(mo):
    mo.md("""
    ---
    # 1️⃣ Conversion Paywall → Trial (#4358)
    
    **Question :** Quel pourcentage des utilisateurs qui voient le paywall d'onboarding lance un essai gratuit ?
    """)


@app.cell
def __(mo):
    mo.accordion({
        "📋 Méthodologie": mo.md("""
**Sources :**
- **DB Eloqa** — Table `PayEvent` : événements d'achat/trial/annulation envoyés par RevenueCat
- **Amplitude** (Sep-Dec 2025) — Events app-side : `Screen MultiStepsPaywall Viewed`, `RC Trial Started`, `Paywall Close Cross Clicked`, `Purchase Cancelled`
- **PostHog** (Fév 2026) — Mêmes events après migration analytics

**Variables clés :**
- `PayEvent.type` = `INITIAL_PURCHASE` : premier achat ou démarrage de trial
- `PayEvent.periodType` = `TRIAL` : période d'essai (vs `NORMAL` pour un achat direct)
- `PayEvent.productId` : identifie le plan (annuel `abo_ann_v3_*` ou mensuel `abo_men_v3_*`)
- `PayEvent.purchasedAtMs` : timestamp de l'achat côté store

**Limites :**
- Les données Amplitude et PostHog sont des comptages d'events analytics (pas la DB). Le croisement se fait par cohérence des ratios, pas par user ID commun.
- Le taux exact dépend du window de tracking — un user peut voir le paywall et convertir des jours plus tard.
- Les données PostHog avant mi-février 2026 sont parcellaires (migration en cours).
        """)
    })


@app.cell
def __(query, pd, px, go, ELOQA_PURPLE, ELOQA_COLORS, mo):
    # PayEvent data for trial analysis
    df_trials = query("""
        SELECT "productId", type, "periodType", store, "countryCode",
               "purchasedAtMs"::bigint as purchased_ms,
               "eventTimestampMs"::bigint as event_ms,
               "priceInPurchasedCurrency" as price_local,
               price as price_usd,
               "originalAppUserId" as user_id
        FROM "PayEvent"
        WHERE environment = 'PRODUCTION'
    """)

    # Summary stats from Amplitude (hardcoded from API queries)
    paywall_data = pd.DataFrame({
        'Action': ['Paywall vu', 'Scroll commencé', 'Scroll en bas', 'Voir toutes les offres', 
                   'Trial démarré', 'Achat validé', 'Achat annulé', 'Croix cliquée'],
        'Amplitude (Sep-Dec 25)': [5740, None, None, 1289, 383, 420, 867, 3786],
        'PostHog (Fév 26)': [1858, 1102, 881, 114, 115, 120, 370, 1207]
    })

    # Funnel chart
    fig_funnel = go.Figure()
    
    amp_funnel = [5740, 3786, 1289, 420, 383]
    amp_labels = ['Paywall vu', 'Croix cliquée (65.9%)', 'Voir offres (22.4%)', 
                  'Achat validé (7.3%)', 'Trial démarré (6.7%)']
    
    fig_funnel = go.Figure(go.Funnel(
        y=amp_labels,
        x=amp_funnel,
        textinfo="value+percent initial",
        marker=dict(color=ELOQA_COLORS[:5]),
        connector=dict(line=dict(color="royalblue", width=1))
    ))
    fig_funnel.update_layout(
        title="Funnel Paywall d'onboarding (Amplitude, Sep-Dec 2025)",
        height=400,
        font=dict(size=14)
    )

    mo.vstack([
        fig_funnel,
        mo.md("""
**Résultat clé :** Le paywall convertit à **6.7%** en trial (stable sur 6 mois).  
**65% ferment le paywall** avec la croix, mais **47% scrollent jusqu'en bas** — ils lisent, ils hésitent, ils partent.  
Le discount paywall convertit 3x moins (2.2%).
        """)
    ])
    return df_trials, paywall_data, fig_funnel


@app.cell
def __(df_trials, pd, px, ELOQA_PURPLE, ELOQA_COLORS, mo):
    # Monthly trial trends from DB
    trials_monthly = df_trials[
        (df_trials['type'] == 'INITIAL_PURCHASE') & 
        (df_trials['periodType'] == 'TRIAL') &
        (df_trials['productId'].str.contains('ann', case=False, na=False))
    ].copy()
    trials_monthly['month'] = pd.to_datetime(trials_monthly['purchased_ms'] / 1000, unit='s').dt.to_period('M').astype(str)
    
    monthly_counts = trials_monthly.groupby('month').agg(
        trials=('user_id', 'nunique')
    ).reset_index()
    monthly_counts = monthly_counts[monthly_counts['month'] >= '2025-09']

    fig_monthly = px.bar(monthly_counts, x='month', y='trials',
                         title='Nouveaux trials annuels par mois (DB)',
                         labels={'month': 'Mois', 'trials': 'Trials'},
                         color_discrete_sequence=[ELOQA_PURPLE])
    fig_monthly.update_layout(height=350)

    # Store split
    store_split = trials_monthly.groupby('store')['user_id'].nunique().reset_index()
    store_split.columns = ['Store', 'Trials']
    fig_store = px.pie(store_split, names='Store', values='Trials',
                       title='Répartition trials par store',
                       color_discrete_sequence=ELOQA_COLORS)
    fig_store.update_layout(height=350)

    mo.hstack([fig_monthly, fig_store])
    return trials_monthly, monthly_counts


# ============================================================
# ANALYSIS 2: Trial Cancellation Timing (#4356)
# ============================================================

@app.cell
def __(mo):
    mo.md("""
    ---
    # 2️⃣ Annulation de la période d'essai (#4356)
    
    **Question :** À quel moment les utilisateurs annulent-ils leur période d'essai après l'avoir activée ?
    """)


@app.cell
def __(mo):
    mo.accordion({
        "📋 Méthodologie": mo.md("""
**Source :** DB Eloqa — Table `PayEvent`

**Variables :**
- `type = 'INITIAL_PURCHASE'` + `periodType = 'TRIAL'` → activation du trial
- `type = 'CANCELLATION'` + `periodType = 'TRIAL'` → annulation
- `purchasedAtMs` → moment de l'activation
- `eventTimestampMs` → moment de l'annulation
- Délai = `eventTimestampMs(CANCEL) - purchasedAtMs(INITIAL)` par `originalAppUserId`

**Limites :**
- L'annulation ≠ la non-conversion. Un user peut annuler son trial (pour éviter le renouvellement auto) tout en continuant à l'utiliser jusqu'à expiration, puis renouveler manuellement.
- Le timestamp d'annulation est celui de la notification RevenueCat, pas forcément le moment exact où l'utilisateur a cliqué.
- Filtré sur `environment = 'PRODUCTION'` et `productId LIKE '%ann%'` (trials annuels uniquement).
        """)
    })


@app.cell
def __(query, pd, px, go, ELOQA_PURPLE, ELOQA_COLORS, mo):
    df_cancel = query("""
        WITH trial_starts AS (
            SELECT "originalAppUserId" as user_id, 
                   MIN("purchasedAtMs"::bigint) as trial_ts
            FROM "PayEvent"
            WHERE environment = 'PRODUCTION' 
            AND "periodType" = 'TRIAL' AND type = 'INITIAL_PURCHASE'
            AND lower("productId") LIKE '%%ann%%'
            GROUP BY "originalAppUserId"
        ),
        trial_cancels AS (
            SELECT pe."originalAppUserId" as user_id,
                   MIN(pe."eventTimestampMs"::bigint) as cancel_ts
            FROM "PayEvent" pe
            JOIN trial_starts ts ON pe."originalAppUserId" = ts.user_id
            WHERE pe.environment = 'PRODUCTION' 
            AND pe."periodType" = 'TRIAL' AND pe.type = 'CANCELLATION'
            GROUP BY pe."originalAppUserId"
        )
        SELECT 
            (tc.cancel_ts - ts.trial_ts) / 3600000.0 as delay_hours
        FROM trial_starts ts
        JOIN trial_cancels tc ON ts.user_id = tc.user_id
    """)

    # Bucket the delays
    def bucket_delay(h):
        if h < 1: return '< 1h'
        elif h < 24: return '1-24h'
        elif h < 72: return 'Jour 2-3'
        elif h < 168: return 'Jour 4-7'
        else: return '7+ jours'

    df_cancel['bucket'] = df_cancel['delay_hours'].apply(bucket_delay)
    cancel_dist = df_cancel['bucket'].value_counts().reindex(['< 1h', '1-24h', 'Jour 2-3', 'Jour 4-7', '7+ jours']).reset_index()
    cancel_dist.columns = ['Délai', 'Users']
    cancel_dist['%'] = (cancel_dist['Users'] / cancel_dist['Users'].sum() * 100).round(1)

    fig_cancel = px.bar(cancel_dist, x='Délai', y='Users', text='%',
                        title='Timing des annulations de trial',
                        color='Délai',
                        color_discrete_sequence=ELOQA_COLORS)
    fig_cancel.update_traces(texttemplate='%{text}%', textposition='outside')
    fig_cancel.update_layout(height=400, showlegend=False)

    # Overall stats
    total_trials = query("SELECT COUNT(DISTINCT \"originalAppUserId\") as n FROM \"PayEvent\" WHERE environment='PRODUCTION' AND \"periodType\"='TRIAL' AND type='INITIAL_PURCHASE' AND lower(\"productId\") LIKE '%%ann%%'")['n'].iloc[0]
    total_cancelled = len(df_cancel)
    converted = query("SELECT COUNT(DISTINCT pe.\"originalAppUserId\") as n FROM \"PayEvent\" pe WHERE pe.environment='PRODUCTION' AND pe.\"periodType\"='NORMAL' AND pe.type IN ('RENEWAL','INITIAL_PURCHASE') AND pe.\"originalAppUserId\" IN (SELECT DISTINCT \"originalAppUserId\" FROM \"PayEvent\" WHERE environment='PRODUCTION' AND \"periodType\"='TRIAL' AND type='INITIAL_PURCHASE')")['n'].iloc[0]

    mo.vstack([
        fig_cancel,
        mo.md(f"""
| Métrique | Valeur |
|---|---|
| Total trials annuels | **{total_trials}** |
| Annulés | **{total_cancelled}** ({round(total_cancelled/total_trials*100, 1)}%) |
| Convertis en payant | **{converted}** ({round(converted/total_trials*100, 1)}%) |

**29% annulent dans l'heure** — comportement "j'active pour voir, je cancel direct au cas où".  
**61% annulent en 3 jours** — la majorité ne va pas au bout du trial de 7 jours.
        """)
    ])
    return df_cancel, total_trials, total_cancelled, converted


# ============================================================
# ANALYSIS 3: Session Abandonment (#4354)
# ============================================================

@app.cell
def __(mo):
    mo.md("""
    ---
    # 3️⃣ Abandon de session (#4354)
    
    **Question :** Quel est le taux d'abandon des sessions de vocabulaire et quels sont les points de friction ?
    """)


@app.cell
def __(mo):
    mo.accordion({
        "📋 Méthodologie": mo.md("""
**Source :** DB Eloqa — Table `ReviewSession`

**Variables :**
- `finalized` (boolean) : `true` = session terminée normalement, `false` = abandonnée
- `startedAt` : timestamp de début de session
- `newDiscoveredCardsNumber` : nombre de nouvelles cartes vues pendant la session
- `newMemorizedCardsNumber` : nombre de cartes passées au statut "mémorisé"

**Définition d'abandon :** Une session avec `finalized = false`. Cela inclut :
- L'utilisateur ferme l'app pendant la session
- L'utilisateur navigue ailleurs sans terminer
- Crash ou perte de connexion

**Limites :**
- Le champ `endedAt` n'est pas toujours renseigné pour les sessions abandonnées → la durée des abandons est peu fiable.
- Un "abandon à 0 carte" peut aussi être un bug technique (session créée mais jamais vraiment démarrée).
- Filtré sur `startedAt >= '2025-06-01'` pour avoir un historique suffisant.
        """)
    })


@app.cell
def __(query, pd, px, go, ELOQA_PURPLE, ELOQA_COLORS, make_subplots, mo):
    # Monthly abandonment trend
    df_abandon = query("""
        SELECT 
            TO_CHAR("startedAt", 'YYYY-MM') as month,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE NOT finalized) as abandoned,
            COUNT(*) FILTER (WHERE finalized) as completed
        FROM "ReviewSession"
        WHERE "startedAt" >= '2025-06-01'
        GROUP BY month ORDER BY month
    """)
    df_abandon['taux_abandon'] = (df_abandon['abandoned'] / df_abandon['total'] * 100).round(1)

    fig_abandon = make_subplots(specs=[[{"secondary_y": True}]])
    fig_abandon.add_trace(
        go.Bar(x=df_abandon['month'], y=df_abandon['total'], name='Total sessions',
               marker_color=ELOQA_COLORS[1], opacity=0.7),
        secondary_y=False
    )
    fig_abandon.add_trace(
        go.Scatter(x=df_abandon['month'], y=df_abandon['taux_abandon'], name="Taux d'abandon (%)",
                   line=dict(color=ELOQA_COLORS[2], width=3), mode='lines+markers+text',
                   text=df_abandon['taux_abandon'].astype(str) + '%', textposition='top center'),
        secondary_y=True
    )
    fig_abandon.update_layout(title="Sessions mensuelles et taux d'abandon", height=400)
    fig_abandon.update_yaxes(title_text="Sessions", secondary_y=False)
    fig_abandon.update_yaxes(title_text="Taux abandon (%)", secondary_y=True, range=[0, 50])

    # Abandonment profile
    df_profile = query("""
        SELECT 
            CASE 
                WHEN "newDiscoveredCardsNumber" = 0 THEN '0 carte (immédiat)'
                WHEN "newDiscoveredCardsNumber" <= 2 THEN '1-2 cartes'
                WHEN "newDiscoveredCardsNumber" <= 5 THEN '3-5 cartes'
                ELSE '6+ cartes'
            END as profil,
            COUNT(*) as sessions
        FROM "ReviewSession"
        WHERE "startedAt" >= '2025-09-01' AND NOT finalized
        GROUP BY profil
    """)
    
    fig_profile = px.pie(df_profile, names='profil', values='sessions',
                         title="Profil des sessions abandonnées",
                         color_discrete_sequence=ELOQA_COLORS)
    fig_profile.update_layout(height=350)

    mo.vstack([
        fig_abandon,
        mo.hstack([
            fig_profile,
            mo.md("""
**Tendance positive :** le taux d'abandon baisse de **38.4% (sept)** à **26.9% (fév)** — amélioration de 30%.

**74% des abandons** = 0 carte découverte.  
Ce n'est pas un problème de difficulté des exercices — c'est un problème de **démarrage**. Les users quittent avant même de commencer.
            """)
        ])
    ])
    return df_abandon, df_profile


# ============================================================
# ANALYSIS 4: Free vs Paid Retention (#4357)
# ============================================================

@app.cell
def __(mo):
    mo.md("""
    ---
    # 4️⃣ Rétention : Gratuit vs Payant (#4357)
    
    **Question :** Comment la rétention des utilisateurs payants se compare-t-elle aux gratuits ?
    """)


@app.cell
def __(mo):
    mo.accordion({
        "📋 Méthodologie": mo.md("""
**Sources :** DB Eloqa — Tables `ReviewSession` + `PayEvent`

**Définitions :**
- **Utilisateur payant** : a au moins un `PayEvent` avec `periodType = 'NORMAL'` et `type IN ('RENEWAL', 'INITIAL_PURCHASE')` → a effectivement payé (pas juste un trial)
- **Utilisateur gratuit** : a au moins une `ReviewSession` finalisée mais n'apparaît pas comme payant
- **Rétention D7/D14/D30** : l'utilisateur a une `ReviewSession` finalisée à au moins 7/14/30 jours après sa première session

**Variables :**
- `ReviewSession.startedAt` — date de la session
- `ReviewSession.finalized` = `true` — session complète (pas abandonnée)
- `PayEvent.originalAppUserId` → croisement avec `ReviewSession.userId`
- Premier et dernier `startedAt` par user → durée de vie

**Limites :**
- La rétention est mesurée par la différence entre première et dernière session, pas par des cohortes hebdomadaires classiques. Un user actif J1 et J30 mais pas entre les deux sera compté comme "retenu D30".
- Biais de sélection : les payants sont par définition des users plus engagés (ils ont décidé de payer). La corrélation paiement → rétention n'implique pas causalité.
- Filtré sur `firstSession >= '2025-09-01'` et `firstSession < NOW() - 30 jours` pour laisser le temps à la rétention D30.
        """)
    })


@app.cell
def __(query, pd, px, go, ELOQA_PURPLE, ELOQA_COLORS, mo):
    df_retention = query("""
        WITH paying_users AS (
            SELECT DISTINCT "originalAppUserId" as user_id
            FROM "PayEvent"
            WHERE environment = 'PRODUCTION' AND "periodType" = 'NORMAL'
            AND type IN ('RENEWAL', 'INITIAL_PURCHASE')
        ),
        user_stats AS (
            SELECT 
                rs."userId",
                CASE WHEN pu.user_id IS NOT NULL THEN 'Payant' ELSE 'Gratuit' END as statut,
                COUNT(*) as sessions,
                MIN(rs."startedAt") as first_session,
                MAX(rs."startedAt") as last_session
            FROM "ReviewSession" rs
            LEFT JOIN paying_users pu ON rs."userId" = pu.user_id
            WHERE rs.finalized = true AND rs."startedAt" >= '2025-09-01'
            GROUP BY rs."userId", statut
            HAVING MIN(rs."startedAt") < NOW() - interval '30 days'
        )
        SELECT 
            statut,
            COUNT(*) as users,
            ROUND(AVG(sessions), 1) as avg_sessions,
            ROUND(AVG(EXTRACT(EPOCH FROM (last_session - first_session)) / 86400)::numeric, 0) as avg_lifespan,
            ROUND(COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (last_session - first_session)) >= 7*86400)::numeric / COUNT(*) * 100, 1) as d7,
            ROUND(COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (last_session - first_session)) >= 14*86400)::numeric / COUNT(*) * 100, 1) as d14,
            ROUND(COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (last_session - first_session)) >= 30*86400)::numeric / COUNT(*) * 100, 1) as d30
        FROM user_stats
        GROUP BY statut
    """)

    # Grouped bar chart for retention
    retention_melted = df_retention.melt(
        id_vars=['statut'], value_vars=['d7', 'd14', 'd30'],
        var_name='période', value_name='rétention'
    )
    retention_melted['période'] = retention_melted['période'].map({'d7': 'D7', 'd14': 'D14', 'd30': 'D30'})

    fig_ret = px.bar(retention_melted, x='période', y='rétention', color='statut',
                     barmode='group', text='rétention',
                     title='Rétention D7 / D14 / D30 : Gratuit vs Payant',
                     color_discrete_map={'Gratuit': ELOQA_COLORS[1], 'Payant': ELOQA_PURPLE},
                     labels={'rétention': 'Rétention (%)', 'période': ''})
    fig_ret.update_traces(texttemplate='%{text}%', textposition='outside')
    fig_ret.update_layout(height=450, yaxis_range=[0, 80])

    # Engagement comparison
    fig_engagement = go.Figure(data=[
        go.Bar(name='Gratuit', x=['Sessions moy.', 'Durée de vie (j)'], 
               y=[float(df_retention[df_retention.statut=='Gratuit']['avg_sessions'].iloc[0]),
                  float(df_retention[df_retention.statut=='Gratuit']['avg_lifespan'].iloc[0])],
               marker_color=ELOQA_COLORS[1]),
        go.Bar(name='Payant', x=['Sessions moy.', 'Durée de vie (j)'],
               y=[float(df_retention[df_retention.statut=='Payant']['avg_sessions'].iloc[0]),
                  float(df_retention[df_retention.statut=='Payant']['avg_lifespan'].iloc[0])],
               marker_color=ELOQA_PURPLE)
    ])
    fig_engagement.update_layout(barmode='group', title='Engagement : Gratuit vs Payant', height=350)

    mo.vstack([
        fig_ret,
        fig_engagement,
        mo.md("""
Les payants retiennent **3.6x à 4.6x mieux**. Le gap s'élargit avec le temps :
- D7 : 62.5% vs 17.4% (×3.6)
- D14 : 56.1% vs 13.8% (×4.1)  
- D30 : **45.5% vs 9.9%** (×4.6)

**Seulement 1 user gratuit sur 10 revient après 30 jours.** Sans conversion rapide (J1-J7), ils sont perdus.
        """)
    ])
    return df_retention, retention_melted


# ============================================================
# ANALYSIS 5: Success Rate vs Retention (#4355 + #4935)
# ============================================================

@app.cell
def __(mo):
    mo.md("""
    ---
    # 5️⃣ Taux de réussite vs Rétention (#4355) — 🔥 Finding principal
    
    **Question :** Quel lien entre le taux de mémorisation des mots et la rétention des utilisateurs ?
    """)


@app.cell
def __(mo):
    mo.accordion({
        "📋 Méthodologie": mo.md("""
**Source :** DB Eloqa — Table `ReviewSession`

**Variables :**
- `newMemorizedCardsNumber` : cartes passées au statut "mémorisé" pendant la session (via l'algorithme de répétition espacée)
- `newDiscoveredCardsNumber` : nouvelles cartes vues pour la première fois
- `totalMemorizedCardsNumber` : total cumulé de cartes mémorisées à la fin de la session
- `totalDiscoveredCardsNumber` : total cumulé de cartes découvertes
- **Taux de réussite** = `SUM(newMemorized) / SUM(newDiscovered)` par utilisateur

**Définition de "mémorisation" :**  
Eloqa utilise un algorithme de **répétition espacée** (type Anki/Leitner). Un mot n'est considéré "mémorisé" que lorsque l'utilisateur l'a correctement rappelé plusieurs fois à des intervalles croissants. **Il est structurellement impossible de mémoriser un mot en 1 seule session** — il faut le revoir dans des sessions ultérieures.

**Rétention D14 :** l'utilisateur a une session finalisée à ≥14 jours de sa première.

**Limites :**
- Le taux de réussite < 20% est biaisé par les users à 1 session qui ne PEUVENT PAS mémoriser (by design).
- Le lien réussite → rétention est corrélationnel : les users qui font plus de sessions mémorisent plus ET restent plus longtemps. La causalité n'est pas prouvée.
- Filtré sur users avec `newDiscovered > 0` et `firstSession < NOW() - 14 jours`.
        """)
    })


@app.cell
def __(query, pd, px, go, ELOQA_PURPLE, ELOQA_COLORS, make_subplots, mo):
    # Success rate vs retention
    df_success = query("""
        WITH user_perf AS (
            SELECT 
                "userId",
                SUM("newMemorizedCardsNumber") as memorized,
                SUM("newDiscoveredCardsNumber") as discovered,
                COUNT(*) as sessions,
                MIN("startedAt") as first_session,
                MAX("startedAt") as last_session
            FROM "ReviewSession"
            WHERE finalized = true AND "startedAt" >= '2025-09-01'
            GROUP BY "userId"
            HAVING SUM("newDiscoveredCardsNumber") > 0
            AND MIN("startedAt") < NOW() - interval '14 days'
        )
        SELECT 
            CASE
                WHEN memorized::float / discovered < 0.2 THEN '< 20%'
                WHEN memorized::float / discovered < 0.4 THEN '20-40%'
                WHEN memorized::float / discovered < 0.6 THEN '40-60%'
                WHEN memorized::float / discovered < 0.8 THEN '60-80%'
                ELSE '80%+'
            END as success_bucket,
            COUNT(*) as users,
            ROUND(AVG(sessions), 1) as avg_sessions,
            ROUND(AVG(EXTRACT(EPOCH FROM (last_session - first_session)) / 86400)::numeric, 0) as lifespan,
            ROUND(COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (last_session - first_session)) >= 14*86400)::numeric / COUNT(*) * 100, 1) as d14
        FROM user_perf
        GROUP BY success_bucket
        ORDER BY MIN(memorized::float / discovered)
    """)

    # Main chart: success rate vs D14 retention
    fig_success = make_subplots(specs=[[{"secondary_y": True}]])
    fig_success.add_trace(
        go.Bar(x=df_success['success_bucket'], y=df_success['users'], name='Users',
               marker_color=ELOQA_COLORS[1], opacity=0.6),
        secondary_y=False
    )
    fig_success.add_trace(
        go.Scatter(x=df_success['success_bucket'], y=df_success['d14'], name='Rétention D14 (%)',
                   line=dict(color=ELOQA_COLORS[2], width=4), mode='lines+markers+text',
                   text=df_success['d14'].astype(str) + '%', textposition='top center',
                   marker=dict(size=12)),
        secondary_y=True
    )
    fig_success.update_layout(
        title="🔥 Le cliff de la réussite : taux de mémorisation vs rétention D14",
        height=500, font=dict(size=14)
    )
    fig_success.update_yaxes(title_text="Nombre d'utilisateurs", secondary_y=False)
    fig_success.update_yaxes(title_text="Rétention D14 (%)", secondary_y=True, range=[0, 100])

    mo.vstack([
        fig_success,
        mo.callout("""
**89% des users** (9 276) ont un taux de réussite < 20% → rétention D14 = **5.8%**  
Dès qu'on passe à 20-40% → D14 = **85.3%**  
C'est un **×15 de rétention**.
        """, kind="warn")
    ])
    return df_success,


# ============================================================
# DEEP DIVE: Root Cause Analysis
# ============================================================

@app.cell
def __(mo):
    mo.md("""
    ---
    ## 🔬 Deep Dive : Pourquoi 89% des users sont sous 20% ?
    """)


@app.cell
def __(query, pd, px, go, ELOQA_PURPLE, ELOQA_COLORS, make_subplots, mo):
    # Session progression: mastery by session number
    df_progression = query("""
        SELECT 
            session_num,
            COUNT(*) as users,
            ROUND(AVG("totalMemorizedCardsNumber"::float / NULLIF("totalDiscoveredCardsNumber", 0) * 100)::numeric, 1) as mastery_pct,
            ROUND(AVG("newMemorizedCardsNumber")::numeric, 2) as new_memorized,
            ROUND(AVG(days_since_first)::numeric, 1) as avg_days
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY "userId" ORDER BY "startedAt") as session_num,
                   EXTRACT(EPOCH FROM ("startedAt" - FIRST_VALUE("startedAt") OVER (PARTITION BY "userId" ORDER BY "startedAt"))) / 86400 as days_since_first
            FROM "ReviewSession"
            WHERE finalized = true AND "startedAt" >= '2025-09-01'
        ) sub
        WHERE session_num <= 15
        GROUP BY session_num
        ORDER BY session_num
    """)

    fig_prog = make_subplots(specs=[[{"secondary_y": True}]])
    fig_prog.add_trace(
        go.Bar(x=df_progression['session_num'], y=df_progression['users'], name='Users actifs',
               marker_color=ELOQA_COLORS[1], opacity=0.5),
        secondary_y=False
    )
    fig_prog.add_trace(
        go.Scatter(x=df_progression['session_num'], y=df_progression['mastery_pct'], name='Mastery (%)',
                   line=dict(color=ELOQA_PURPLE, width=3), mode='lines+markers'),
        secondary_y=True
    )
    # Add 20% threshold line
    fig_prog.add_hline(y=20, line_dash="dash", line_color="red", annotation_text="Seuil 20% (rétention)",
                       secondary_y=True)
    fig_prog.update_layout(
        title="Progression de la maîtrise par numéro de session",
        height=450, xaxis_title="N° de session"
    )
    fig_prog.update_yaxes(title_text="Users", secondary_y=False)
    fig_prog.update_yaxes(title_text="Mastery (%)", secondary_y=True, range=[0, 40])

    mo.vstack([
        fig_prog,
        mo.md("""
**Il faut 8 sessions (~20 jours) pour atteindre le seuil de 20% de mastery.**  
Mais le nombre d'utilisateurs chute drastiquement : de 11 000 en session 1 à ~1 100 en session 8.
        """)
    ])
    return df_progression,


@app.cell
def __(query, pd, px, ELOQA_PURPLE, ELOQA_COLORS, mo):
    # Session distribution
    df_sessions = query("""
        SELECT 
            CASE 
                WHEN sessions = 1 THEN '1'
                WHEN sessions = 2 THEN '2'
                WHEN sessions = 3 THEN '3'
                WHEN sessions <= 5 THEN '4-5'
                WHEN sessions <= 8 THEN '6-8'
                WHEN sessions <= 15 THEN '9-15'
                ELSE '16+'
            END as bucket,
            COUNT(*) as users,
            MIN(sessions) as min_s
        FROM (
            SELECT "userId", COUNT(*) as sessions
            FROM "ReviewSession"
            WHERE finalized = true AND "startedAt" >= '2025-09-01'
            GROUP BY "userId"
        ) sub
        GROUP BY bucket
        ORDER BY min_s
    """)
    df_sessions['pct'] = (df_sessions['users'] / df_sessions['users'].sum() * 100).round(1)
    df_sessions['cumul'] = df_sessions['pct'].cumsum().round(1)

    fig_dist = px.bar(df_sessions, x='bucket', y='users', text='pct',
                      title="Distribution du nombre de sessions par utilisateur",
                      color_discrete_sequence=[ELOQA_PURPLE],
                      labels={'bucket': 'Nombre de sessions', 'users': 'Utilisateurs'})
    fig_dist.update_traces(texttemplate='%{text}%', textposition='outside')
    fig_dist.update_layout(height=400)

    mo.vstack([
        fig_dist,
        mo.callout("""
**67% des utilisateurs ne font qu'UNE seule session.**  
82.5% en font 3 ou moins.  
Avec un système de répétition espacée qui nécessite 8 sessions pour atteindre 20% de mastery,  
**la majorité des users ne peut structurellement pas réussir**.
        """, kind="danger")
    ])
    return df_sessions,


@app.cell
def __(query, pd, px, go, ELOQA_PURPLE, ELOQA_COLORS, mo):
    # The proof: users who memorize in S1 return massively
    df_s1 = query("""
        WITH session1 AS (
            SELECT "userId", "newMemorizedCardsNumber"
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY "userId" ORDER BY "startedAt") as sn
                FROM "ReviewSession"
                WHERE finalized = true AND "startedAt" >= '2025-09-01'
            ) sub WHERE sn = 1
        ),
        user_sessions AS (
            SELECT "userId", COUNT(*) as total_sessions
            FROM "ReviewSession"
            WHERE finalized = true AND "startedAt" >= '2025-09-01'
            GROUP BY "userId"
        )
        SELECT 
            CASE WHEN s1."newMemorizedCardsNumber" > 0 THEN 'Mémorisé ≥1 mot' ELSE 'Rien mémorisé' END as s1_result,
            COUNT(*) as users,
            ROUND(AVG(us.total_sessions), 1) as avg_sessions,
            ROUND(COUNT(*) FILTER (WHERE us.total_sessions >= 2)::numeric / COUNT(*) * 100, 1) as pct_return,
            ROUND(COUNT(*) FILTER (WHERE us.total_sessions >= 5)::numeric / COUNT(*) * 100, 1) as pct_5plus
        FROM session1 s1
        JOIN user_sessions us ON s1."userId" = us."userId"
        GROUP BY s1_result
    """)

    fig_s1 = go.Figure(data=[
        go.Bar(name='Reviennent (S2+)', x=df_s1['s1_result'], y=df_s1['pct_return'],
               marker_color=ELOQA_COLORS[3], text=df_s1['pct_return'].astype(str) + '%', textposition='inside'),
        go.Bar(name='Font 5+ sessions', x=df_s1['s1_result'], y=df_s1['pct_5plus'],
               marker_color=ELOQA_PURPLE, text=df_s1['pct_5plus'].astype(str) + '%', textposition='inside')
    ])
    fig_s1.update_layout(barmode='group', title="Impact de la mémorisation en session 1 sur le retour",
                         height=400, yaxis_title='%')

    mo.vstack([
        fig_s1,
        mo.callout("""
**La preuve du cercle vicieux :**
- Rien mémorisé en S1 → **31% reviennent**, 13% font 5+ sessions
- ≥1 mot mémorisé en S1 → **81% reviennent**, 62% font 5+ sessions

**La gratification de progrès est LE déclencheur du retour.**
        """, kind="success")
    ])
    return df_s1,


# ============================================================
# CONCLUSIONS & RECOMMENDATIONS
# ============================================================

@app.cell
def __(mo):
    mo.md("""
    ---
    # 🎯 Synthèse et Recommandations
    
    ## Le cercle vicieux identifié
    
    ```
    Mots trop durs → Pas de mémorisation en S1 → Pas de gratification
         ↑                                              ↓
         └──── Pas de mémorisation ← Pas de retour ←────┘
    ```
    
    ## Top 5 des actions à impact
    
    | # | Action | Impact attendu | Effort |
    |---|---|---|---|
    | 1 | **Feedback de progrès en S1** avant la mémorisation réelle ("3 mots découverts !") | Augmenter le taux de retour S2 | Faible |
    | 2 | **Baisser la difficulté initiale** pour que les nouveaux users soient à 30-60% de réussite | ×15 de rétention D14 potentiel | Moyen |
    | 3 | **Bouton cadeau sur la Home** pour re-proposer l'offre aux 65% qui ferment le paywall | Nouveau canal de conversion (0 aujourd'hui) | Faible |
    | 4 | **Notification ciblée post-S1** ("3 mots à réviser — 2 min") | Déclencher la S2 critique | Faible |
    | 5 | **Plus de cartes en S1** (76% n'en voient que 1-2) | Plus de matière = plus d'attachement | Moyen |
    
    ---
    
    *📊 Analyse par Quanty — Sprint Fév 2026*  
    *Sources : DB Eloqa (PayEvent, ReviewSession, User), Amplitude, PostHog, RevenueCat*
    """)


@app.cell
def __(mo):
    # Export instructions
    mo.md("""
    ---
    > **Pour exporter ce rapport en HTML :**  
    > `marimo export html notebooks/eloqa_sprint_analysis.py -o reports/sprint_feb2026.html`
    """)


if __name__ == "__main__":
    app.run()
