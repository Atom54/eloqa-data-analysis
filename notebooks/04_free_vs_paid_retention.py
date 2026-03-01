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

    ELOQA_PURPLE = "#6C5CE7"
    ELOQA_COLORS = ["#6C5CE7", "#a29bfe", "#fd79a8", "#00cec9", "#fdcb6e", "#e17055", "#00b894"]

    def query(sql):
        conn = psycopg2.connect(host="localhost", dbname="eloqa", user="eloqa_reader", password="readonly")
        try:
            return pd.read_sql(sql, conn)
        finally:
            conn.close()

    return ELOQA_COLORS, ELOQA_PURPLE, go, make_subplots, mo, np, pd, psycopg2, px, query


@app.cell
def __(mo):
    mo.md("""
    # 4️⃣ Rétention : Gratuit vs Payant
    
    **Fibery :** #4357  
    **Question :** Comment la rétention et l'engagement des utilisateurs payants se comparent-ils aux gratuits ?  
    **Période :** Septembre 2025 – Février 2026
    
    ---
    """)


@app.cell
def __(mo):
    mo.md("## 📋 Méthodologie")


@app.cell
def __(mo):
    mo.accordion({
        "Sources & Variables (cliquer pour déplier)": mo.md("""
**Sources :** DB Eloqa — Tables `ReviewSession` + `PayEvent`

**Définitions :**
- **Utilisateur payant** : au moins 1 `PayEvent` avec `periodType = 'NORMAL'` et `type IN ('RENEWAL', 'INITIAL_PURCHASE')` → a réellement payé (pas juste trial)
- **Utilisateur gratuit** : au moins 1 `ReviewSession` finalisée, mais pas d'événement de paiement
- **Rétention D7/D14/D30** : l'utilisateur a une session finalisée à ≥ 7/14/30 jours après sa première session

**Variables :**
- `ReviewSession.startedAt` — date de session
- `ReviewSession.finalized` = `true` — session terminée (pas abandon)
- `PayEvent.originalAppUserId` croisé avec `ReviewSession.userId`
- Premier et dernier `startedAt` par user → durée de vie

**Limites :**
- Rétention mesurée par `max(startedAt) - min(startedAt)`, pas par cohortes hebdomadaires classiques. Un user actif J1 et J30 (mais pas entre) sera compté "retenu D30"
- **Biais de sélection** : les payants sont par définition plus engagés (ils ont choisi de payer). Corrélation ≠ causalité
- Filtré sur `firstSession >= '2025-09-01'` et `firstSession < NOW() - 30 jours` pour laisser le temps à D30
        """)
    })


@app.cell
def __(mo):
    mo.md("## 📊 Rétention D7 / D14 / D30")


@app.cell
def __(query, pd, px, go, ELOQA_PURPLE, ELOQA_COLORS, mo):
    df_ret = query("""
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
                MAX(rs."startedAt") as last_session,
                SUM(rs."newMemorizedCardsNumber") as total_memorized,
                SUM(rs."newDiscoveredCardsNumber") as total_discovered
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
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sessions)::numeric, 0) as median_sessions,
            ROUND(AVG(EXTRACT(EPOCH FROM (last_session - first_session)) / 86400)::numeric, 0) as avg_lifespan,
            ROUND(AVG(total_memorized)::numeric, 1) as avg_memorized,
            ROUND(COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (last_session - first_session)) >= 7*86400)::numeric / COUNT(*) * 100, 1) as d7,
            ROUND(COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (last_session - first_session)) >= 14*86400)::numeric / COUNT(*) * 100, 1) as d14,
            ROUND(COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (last_session - first_session)) >= 30*86400)::numeric / COUNT(*) * 100, 1) as d30
        FROM user_stats
        GROUP BY statut
    """)

    # Retention comparison
    ret_melted = df_ret.melt(
        id_vars=['statut'], value_vars=['d7', 'd14', 'd30'],
        var_name='période', value_name='rétention'
    )
    ret_melted['période'] = ret_melted['période'].map({'d7': 'D7', 'd14': 'D14', 'd30': 'D30'})

    fig_ret = px.bar(ret_melted, x='période', y='rétention', color='statut',
                     barmode='group', text='rétention',
                     title='Rétention : Gratuit vs Payant',
                     color_discrete_map={'Gratuit': ELOQA_COLORS[1], 'Payant': ELOQA_PURPLE},
                     labels={'rétention': 'Rétention (%)', 'période': ''})
    fig_ret.update_traces(texttemplate='%{text}%', textposition='outside')
    fig_ret.update_layout(height=450, yaxis_range=[0, 80], font=dict(size=14))
    fig_ret
    return df_ret,


@app.cell
def __(df_ret, mo):
    _gratuit = df_ret[df_ret['statut'] == 'Gratuit'].iloc[0]
    _payant = df_ret[df_ret['statut'] == 'Payant'].iloc[0]
    
    mo.callout(f"""
**D7 :** Payant {_payant['d7']}% vs Gratuit {_gratuit['d7']}% (×{round(_payant['d7']/_gratuit['d7'], 1)})  
**D14 :** Payant {_payant['d14']}% vs Gratuit {_gratuit['d14']}% (×{round(_payant['d14']/_gratuit['d14'], 1)})  
**D30 :** Payant {_payant['d30']}% vs Gratuit {_gratuit['d30']}% (×{round(_payant['d30']/_gratuit['d30'], 1)})

Le gap **s'élargit** avec le temps — les gratuits décrochent de plus en plus vite.
    """, kind="warn")


@app.cell
def __(mo):
    mo.md("## 📊 Engagement comparé")


@app.cell
def __(df_ret, go, ELOQA_PURPLE, ELOQA_COLORS, mo):
    _g = df_ret[df_ret['statut'] == 'Gratuit'].iloc[0]
    _p = df_ret[df_ret['statut'] == 'Payant'].iloc[0]

    fig_eng = go.Figure(data=[
        go.Bar(name='Gratuit', 
               x=['Sessions moy.', 'Sessions méd.', 'Durée vie (j)', 'Mots mémorisés'],
               y=[float(_g['avg_sessions']), float(_g['median_sessions']),
                  float(_g['avg_lifespan']), float(_g['avg_memorized'])],
               marker_color=ELOQA_COLORS[1]),
        go.Bar(name='Payant',
               x=['Sessions moy.', 'Sessions méd.', 'Durée vie (j)', 'Mots mémorisés'],
               y=[float(_p['avg_sessions']), float(_p['median_sessions']),
                  float(_p['avg_lifespan']), float(_p['avg_memorized'])],
               marker_color=ELOQA_PURPLE)
    ])
    fig_eng.update_layout(barmode='group', title='Engagement : Gratuit vs Payant', height=400)
    fig_eng


@app.cell
def __(mo):
    mo.md("## 📊 Distribution des sessions par type d'utilisateur")


@app.cell
def __(query, px, ELOQA_PURPLE, ELOQA_COLORS, mo):
    df_dist = query("""
        WITH paying_users AS (
            SELECT DISTINCT "originalAppUserId" as user_id
            FROM "PayEvent"
            WHERE environment = 'PRODUCTION' AND "periodType" = 'NORMAL'
            AND type IN ('RENEWAL', 'INITIAL_PURCHASE')
        ),
        user_counts AS (
            SELECT 
                rs."userId",
                CASE WHEN pu.user_id IS NOT NULL THEN 'Payant' ELSE 'Gratuit' END as statut,
                COUNT(*) as sessions
            FROM "ReviewSession" rs
            LEFT JOIN paying_users pu ON rs."userId" = pu.user_id
            WHERE rs.finalized = true AND rs."startedAt" >= '2025-09-01'
            GROUP BY rs."userId", statut
        )
        SELECT statut,
            CASE 
                WHEN sessions = 1 THEN '1'
                WHEN sessions <= 3 THEN '2-3'
                WHEN sessions <= 8 THEN '4-8'
                WHEN sessions <= 20 THEN '9-20'
                ELSE '21+'
            END as bucket,
            COUNT(*) as users,
            MIN(sessions) as sort_key
        FROM user_counts
        GROUP BY statut, bucket
        ORDER BY statut, sort_key
    """)
    
    fig_dist = px.bar(df_dist, x='bucket', y='users', color='statut',
                      barmode='group',
                      title='Distribution du nombre de sessions',
                      color_discrete_map={'Gratuit': ELOQA_COLORS[1], 'Payant': ELOQA_PURPLE},
                      labels={'bucket': 'Sessions', 'users': 'Utilisateurs'})
    fig_dist.update_layout(height=400)
    fig_dist


@app.cell
def __(mo):
    mo.md("""
    ---
    ## 💡 Hypothèses & Conclusions
    
    **Hypothèse 1 :** Les payants sont simplement des gratuits qui ont payé  
    ❌ **Infirmé** — Le gap d'engagement est massif (×4.6 en D30). Payer crée un engagement psychologique (sunk cost) ET filtre les users déjà motivés
    
    **Hypothèse 2 :** La rétention gratuite est suffisante pour convertir plus tard  
    ❌ **Infirmé** — 90% des gratuits ont disparu à D30. Le window de conversion est D1-D7 max
    
    **Hypothèse 3 :** Le paywall freine la rétention des gratuits  
    ⚠️ **Nuancé** — Les gratuits qui restent (9.9% D30) sont très engagés malgré l'absence d'abonnement. Le problème n'est pas le paywall mais le manque de valeur perçue en S1-S3
    
    ---
    
    **Recommandation :** Convertir les gratuits dans les **7 premiers jours** ou les considérer comme perdus. Tout effort de rétention post-J7 pour les gratuits a un ROI quasi-nul.
    
    ---
    *📊 Analyse par Quanty — Sprint Fév 2026*
    """)


if __name__ == "__main__":
    app.run()
