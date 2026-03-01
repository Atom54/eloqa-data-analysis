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
    # 3️⃣ Abandon de session
    
    **Fibery :** #4354  
    **Question :** Quel est le taux d'abandon des sessions de vocabulaire et où sont les points de friction ?  
    **Période :** Juin 2025 – Février 2026
    
    ---
    """)


@app.cell
def __(mo):
    mo.md("## 📋 Méthodologie")


@app.cell
def __(mo):
    mo.accordion({
        "Sources & Variables (cliquer pour déplier)": mo.md("""
**Source :** DB Eloqa — Table `ReviewSession`

**Variables :**
- `ReviewSession.finalized` (boolean) : `true` = session terminée normalement, `false` = abandon
- `ReviewSession.startedAt` : timestamp de début
- `ReviewSession.endedAt` : timestamp de fin (pas toujours renseigné pour les abandons)
- `ReviewSession.newDiscoveredCardsNumber` : cartes découvertes pendant la session
- `ReviewSession.newMemorizedCardsNumber` : cartes mémorisées

**Définition d'abandon :** `finalized = false`. Inclut :
- Fermeture de l'app en cours de session
- Navigation ailleurs (Home, profil...)
- Crash ou perte de connexion
- Bug technique (session créée mais jamais vraiment démarrée)

**Limites :**
- `endedAt` souvent NULL pour les sessions abandonnées → durée des abandons peu fiable
- Un "abandon à 0 carte" peut être un bug technique (session créée côté serveur sans que l'UI démarre réellement)
- Pas de distinction entre abandon volontaire et crash technique
        """)
    })


@app.cell
def __(mo):
    mo.md("## 📊 Tendance mensuelle")


@app.cell
def __(query, pd, go, ELOQA_COLORS, make_subplots, mo):
    df_monthly = query("""
        SELECT 
            TO_CHAR("startedAt", 'YYYY-MM') as month,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE NOT finalized) as abandoned,
            COUNT(*) FILTER (WHERE finalized) as completed
        FROM "ReviewSession"
        WHERE "startedAt" >= '2025-06-01'
        GROUP BY month ORDER BY month
    """)
    df_monthly['taux_abandon'] = (df_monthly['abandoned'] / df_monthly['total'] * 100).round(1)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=df_monthly['month'], y=df_monthly['completed'], name='Finalisées',
               marker_color=ELOQA_COLORS[6], opacity=0.7),
        secondary_y=False
    )
    fig.add_trace(
        go.Bar(x=df_monthly['month'], y=df_monthly['abandoned'], name='Abandonnées',
               marker_color=ELOQA_COLORS[2], opacity=0.7),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=df_monthly['month'], y=df_monthly['taux_abandon'], 
                   name="Taux d'abandon (%)",
                   line=dict(color='black', width=3), mode='lines+markers+text',
                   text=df_monthly['taux_abandon'].astype(str) + '%', textposition='top center'),
        secondary_y=True
    )
    fig.update_layout(title="Sessions mensuelles — finalisées vs abandonnées", 
                      height=450, barmode='stack')
    fig.update_yaxes(title_text="Sessions", secondary_y=False)
    fig.update_yaxes(title_text="Taux abandon (%)", secondary_y=True, range=[0, 50])
    fig
    return df_monthly,


@app.cell
def __(mo):
    mo.callout("""
**Tendance positive :** le taux d'abandon passe de **38.4% (sept 2025)** à **26.9% (fév 2026)** — amélioration de 30%.  
Le nombre de sessions augmente en parallèle → l'engagement s'améliore.
    """, kind="success")


@app.cell
def __(mo):
    mo.md("## 📊 Profil des sessions abandonnées")


@app.cell
def __(query, px, ELOQA_COLORS, mo):
    df_profile = query("""
        SELECT 
            CASE 
                WHEN "newDiscoveredCardsNumber" = 0 THEN '0 carte (immédiat)'
                WHEN "newDiscoveredCardsNumber" <= 2 THEN '1-2 cartes'
                WHEN "newDiscoveredCardsNumber" <= 5 THEN '3-5 cartes'
                WHEN "newDiscoveredCardsNumber" <= 10 THEN '6-10 cartes'
                ELSE '11+ cartes'
            END as profil,
            COUNT(*) as sessions,
            MIN("newDiscoveredCardsNumber") as min_cards
        FROM "ReviewSession"
        WHERE "startedAt" >= '2025-09-01' AND NOT finalized
        GROUP BY profil
        ORDER BY min_cards
    """)
    df_profile['pct'] = (df_profile['sessions'] / df_profile['sessions'].sum() * 100).round(1)

    fig_profile = px.bar(df_profile, x='profil', y='sessions', text='pct',
                         title="Nombre de cartes découvertes avant abandon",
                         color='profil', color_discrete_sequence=ELOQA_COLORS)
    fig_profile.update_traces(texttemplate='%{text}%', textposition='outside')
    fig_profile.update_layout(height=420, showlegend=False, xaxis_title='Cartes vues')
    fig_profile


@app.cell
def __(mo):
    mo.callout("""
**74% des abandons = 0 carte découverte.**  
Ce n'est pas un problème de difficulté des exercices — c'est un problème de **démarrage**.  
Les users quittent avant même de commencer la première carte.
    """, kind="danger")


@app.cell
def __(mo):
    mo.md("## 📊 Abandon par numéro de session")


@app.cell
def __(query, px, ELOQA_PURPLE, ELOQA_COLORS, mo):
    df_by_sn = query("""
        WITH numbered AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY "userId" ORDER BY "startedAt") as sn
            FROM "ReviewSession"
            WHERE "startedAt" >= '2025-09-01'
        )
        SELECT 
            CASE WHEN sn <= 5 THEN sn::text ELSE '6+' END as session_num,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE NOT finalized) as abandoned,
            MIN(sn) as sort_key
        FROM numbered
        GROUP BY session_num
        ORDER BY sort_key
    """)
    df_by_sn['taux'] = (df_by_sn['abandoned'] / df_by_sn['total'] * 100).round(1)

    fig_sn = px.bar(df_by_sn, x='session_num', y='taux', text='taux',
                    title="Taux d'abandon par numéro de session",
                    color_discrete_sequence=[ELOQA_PURPLE],
                    labels={'session_num': 'N° de session', 'taux': "Taux d'abandon (%)"})
    fig_sn.update_traces(texttemplate='%{text}%', textposition='outside')
    fig_sn.update_layout(height=380)
    fig_sn


@app.cell
def __(mo):
    mo.md("""
    ---
    ## 💡 Hypothèses & Conclusions
    
    **Hypothèse 1 :** Les exercices sont trop difficiles, les users abandonnent en cours  
    ❌ **Infirmé** — 74% abandonnent à 0 carte. Ils ne voient même pas un exercice
    
    **Hypothèse 2 :** L'abandon est un problème de chargement ou de bug technique  
    ⚠️ **Possible** — Un taux de 74% à 0 carte est anormalement haut. Certains abandons sont probablement des bugs (session créée côté serveur sans que l'UI se charge)
    
    **Hypothèse 3 :** Le taux d'abandon est stable  
    ❌ **Infirmé** — Baisse significative de 38% à 27% entre sept et fév. L'amélioration est réelle et continue
    
    ---
    
    **Recommandations :**
    - **Investiguer les abandons à 0 carte** — distinguer abandon volontaire vs bug technique (ajouter un event analytics `session_ui_loaded`)
    - **Charger les premières cartes plus vite** — si c'est un problème de temps de chargement, ça explique les 74%
    - **Continuer la tendance** — identifier quels changements récents ont causé la baisse (nouvelle UI ? meilleur onboarding ?)
    
    ---
    *📊 Analyse par Quanty — Sprint Fév 2026*
    """)


if __name__ == "__main__":
    app.run()
