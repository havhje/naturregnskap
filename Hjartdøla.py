import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium", layout_file="layouts/Hjartdøla.slides.json")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    from PIL import Image
    import great_tables as gt
    from great_tables import html
    import numpy as np
    return Image, alt, mo, pl


@app.cell(hide_code=True)
def _(mo):
    hjartdola_df = mo.sql(
        f"""
        SELECT * FROM 'Hjartdøla\hjartdøla.csv';
        """,
        output=False,
    )
    return (hjartdola_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    #Hjartdøla 
    ##Case naturregnskap - Foreløpige resultater og vurderinger
    """
    )
    return


@app.cell(hide_code=True)
def _(Image, mo):
    # Load and display the image
    image_path = "Hjartdøla\oversikthjartdøla.png"
    img = Image.open(image_path)


    mo.vstack(
        [
            mo.hstack([mo.md("### Oversikt Hjartdøla")]),
            mo.hstack([mo.image(img)]),
        ]
    )
    return


@app.cell(hide_code=True)
def _(Image, mo):
    # Load and display the image
    image_path1 = "Hjartdøla\dn13.png"
    img1 = Image.open(image_path1)


    mo.vstack(
        [
            mo.hstack([mo.md("### Oversikt Hjartdøla")]),
            mo.hstack([mo.image(img1)]),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Naturpoeng før inngrep


    $$\text{Naturpoeng (før)} = A_{t0} \times Nk_{t0} \times Nf_{t0}$$

    **Hvor:**

    $A_{t0}$ = Areal (daa) eller lineær vannforekomst (m) før tiltak

    $Nk_{t0}$ = Naturkvalitet før tiltak (**for vann; Nk pr. delstrekning[skjønnsvurdering]**)

    $Nf_{t0}$ = Forvaltningsinteresse før tiltak (**for vann; Nf pr. delstrekning[skjønnsvurdering]**)

    ---

    ## Naturpoeng tapt

    $$\text{Naturpoeng (tapt)} = A_{\text{tapt}} \times Nk_{t0} \times Nf_{t0} + (A_{\text{rest}} \times Nk_{t0} \times Nf_{t0}) \times P$$

    **Hvor:**

    - $A_{\text{tapt}}$ = Nedbygd areal (daa) eller ødelagt lineær vannforekomst (delstrekning i m)

    - $A_{\text{rest}}$ = Resterende areal (daa) og lineære vannforekomster (m) som er påvirket på andre måter enn direkte arealtap

    - $Nk_{t0}$ = Naturkvalitet før tiltak

    - $Nf_{t0}$ = Forvaltningsinteresse før tiltak

    - $P$ = Påvirkningsfaktor
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ##Bruker denne formlen for å beregne tap for vannmiljø:

    ---

    ### Naturpoeng før inngrep


    $$\text{Naturpoeng (før)} = A_{t0} \times Nk_{t0} \times Nf_{t0}$$

    **Hvor:**

    $A_{t0}$ = Areal (daa) eller lineær vannforekomst (m) før tiltak

    $Nk_{t0}$ = Naturkvalitet før tiltak

    $Nf_{t0}$ = Forvaltningsinteresse før tiltak

    ---

    ### Naturpoeng etter inngrep 

    $$\text{Naturpoeng (etter)} = A_{t1} \times Nk_{t1} \times Nf_{t1}$$

    **Hvor:**

    $A_{t1}$ = Areal (daa) eller lineær vannforekomst (m) etter tiltak

    $Nk_{t1}$ = Naturkvalitet etter tiltak

    $Nf_{t1}$ = Forvaltningsinteresse etter tiltak

    ---

    ### Naturpoeng tapt 

    $$\Large \text{Naturpoeng (tapt)} = (A_{t0} \times Nk_{t0} \times Nf_{t0}) - (A_{t1} \times Nk_{t1} \times Nf_{t1})$$
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Konstante verdier per scenario


    | **Tidspunkt** | **Scenario** | **Naturkvalitet (Nk)** | **Beskrivelse Nk** | **Forvaltningsinteresse (Nf)** | **Beskrivelse Nf** | **Produkt (Nk × Nf)** | **Differanse** |
    |---|---|:---:|---|:---:|---|:---:|:---:|
    | **Før inngrep** | Best Case | 4.0 | Svært god økologisk tilstand | 3.0 | Vassdrag høyproduktive for ørret, røye eller sik | **12.0** | +200% |
    | **Før inngrep** | Worst Case | 4.0 | Svært god økologisk tilstand | 1.0 | Innlandsfisk: små bestander uten spesielle verdier | **4.0** | Baseline |
    | **Etter inngrep** | Best Case | 2.2 | Moderat økologisk tilstand | 3.0 | Vassdrag høyproduktive for ørret, røye eller sik | **6.6** | +560% |
    | **Etter inngrep** | Worst Case | 1.0 | Svært dårlig økologisk tilstand | 1.0 | Innlandsfisk: små bestander uten spesielle verdier | **1.0** | Baseline |


    ### Beregningsformel

    $$\text{Naturpoeng}_{\text{delområde}} = U_{\text{delområde}} \times (Nk \times Nf)_{\text{scenario}}$$
    """
    )
    return


@app.cell(hide_code=True)
def _(alt, hjartdola_df, mo, pl):
    # Prepare data for visualization
    _chart_data = hjartdola_df.with_columns(
        pl.concat_str(
            [pl.col("før/etter"), pl.lit(" - "), pl.col("best_case/worst_case")]
        ).alias("scenario")
    ).select(
        [
            "delområde",
            "scenario",
            "før/etter",
            "best_case/worst_case",
            "naturpoeng_for_inngrep",
        ]
    )

    # Create interactive Altair chart
    _base = alt.Chart(_chart_data.to_pandas()).encode(
        y=alt.Y("delområde:N", title="Delområde", sort=None),
        x=alt.X("naturpoeng_for_inngrep:Q", title="Naturpoeng"),
        color=alt.Color("delområde:N", legend=None),
        opacity=alt.Opacity(
            "før/etter:N",
            scale=alt.Scale(domain=["Før", "Etter"], range=[1, 0.6]),
            legend=alt.Legend(
                title="Tidspunkt", titleFontSize=14, labelFontSize=12
            ),
        ),
        strokeDash=alt.StrokeDash(
            "best_case/worst_case:N",
            scale=alt.Scale(
                domain=["Best case", "Worst case"], range=[[1, 0], [5, 5]]
            ),
            legend=alt.Legend(
                title="Scenario", titleFontSize=14, labelFontSize=12
            ),
        ),
        tooltip=["delområde:N", "scenario:N", "naturpoeng_for_inngrep:Q"],
    )

    _bars = _base.mark_bar(stroke="white", strokeWidth=3, height=25).encode(
        y=alt.Y("scenario:N", title=""),
        row=alt.Row(
            "delområde:N",
            header=alt.Header(
                labelAngle=0, labelAlign="left", title="", labelFontSize=13
            ),
        ),
    )

    _chart = (
        _bars.properties(
            width=2000,
            height=150,
            title=alt.TitleParams(
                text="Naturpoeng per delområde - Før/Etter & Best/Worst Case",
                fontSize=16,
                fontWeight="normal",
            ),
        )
        .configure_facet(spacing=8)
        .configure_axis(labelFontSize=12, titleFontSize=14)
        .configure_legend(
            symbolSize=120,
            symbolStrokeWidth=2,
            padding=10,
            columnPadding=15,
            rowPadding=5,
        )
        .interactive()
    )

    mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(alt, hjartdola_df, mo, pl):
    # Calculate uncertainty vs sensitivity comparison - separated by før/etter
    _comparison_data = (
        hjartdola_df.group_by("delområde")
        .agg(
            [
                pl.col("naturpoeng_for_inngrep")
                .filter(pl.col("før/etter") == "før")
                .max()
                .alias("før_max"),
                pl.col("naturpoeng_for_inngrep")
                .filter(pl.col("før/etter") == "før")
                .min()
                .alias("før_min"),
                pl.col("naturpoeng_for_inngrep")
                .filter(pl.col("før/etter") == "før")
                .mean()
                .alias("før_mean"),
                pl.col("naturpoeng_for_inngrep")
                .filter(pl.col("før/etter") == "etter")
                .max()
                .alias("etter_max"),
                pl.col("naturpoeng_for_inngrep")
                .filter(pl.col("før/etter") == "etter")
                .min()
                .alias("etter_min"),
                pl.col("naturpoeng_for_inngrep")
                .filter(pl.col("før/etter") == "etter")
                .mean()
                .alias("etter_mean"),
            ]
        )
        .with_columns(
            [
                (pl.col("før_max") - pl.col("før_min")).alias("Usikkerhet Før"),
                (pl.col("etter_max") - pl.col("etter_min")).alias(
                    "Usikkerhet Etter"
                ),
                (pl.col("før_mean") * 0.2).alias("Følsomhet Før (±10%)"),
                (pl.col("etter_mean") * 0.2).alias("Følsomhet Etter (±10%)"),
            ]
        )
    )

    # Reshape for visualization
    _chart_data = (
        _comparison_data.select(
            [
                "delområde",
                "Usikkerhet Før",
                "Usikkerhet Etter",
                "Følsomhet Før (±10%)",
                "Følsomhet Etter (±10%)",
            ]
        )
        .unpivot(
            index=["delområde"], variable_name="Type", value_name="Naturpoeng"
        )
        .with_columns(
            [
                pl.when(pl.col("Type").str.contains("Usikkerhet"))
                .then(pl.lit("Usikkerhet"))
                .otherwise(pl.lit("Følsomhet"))
                .alias("Category")
            ]
        )
    )

    # Create bar chart
    chart = (
        alt.Chart(_chart_data.to_pandas())
        .mark_bar()
        .encode(
            x=alt.X("Type:N", title="", axis=alt.Axis(labelAngle=-45)),
            y=alt.Y("Naturpoeng:Q", title="Naturpoeng variasjon"),
            color=alt.Color(
                "Category:N",
                scale=alt.Scale(
                    domain=["Følsomhet", "Usikkerhet"],
                    range=["#3498db", "#e74c3c"],
                ),
                legend=None,
            ),
            column=alt.Column("delområde:N", title="Delområde"),
            tooltip=[
                "delområde",
                "Type",
                alt.Tooltip("Naturpoeng:Q", format=".0f"),
            ],
        )
        .properties(width=200, height=300, title="Usikkerhet vs følsomhet")
    )

    mo.vstack(
        [
            mo.md("""  
    ### Følsomhet
    Følsomhet = Gjennomsnittet av beste og verste scenario for hvert delområde × 0,2 (representerer ±10% variasjon)

    Viktig egenskap ved multiplikativ modell (A × Nk × Nf):

    - 10% økning i areal → 10% økning i naturpoeng
    - 10% økning i naturkvalitet → 10% økning i naturpoeng
    - 10% økning i forvaltningsinteresse → 10% økning i naturpoeng

    **Alle tre parameterne har lik relativ påvirkning**

    Sammenligning i diagrammet:

    - Blå søyler: Teoretisk effekt av ±10% parameterendring (20% totalvariasjon)
    - Røde søyler: Faktisk usikkerhet mellom beste og verste scenario
        """),
            chart,
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Forslag til videre arbeid og detaljering av prosjektet

    ### 1. Fokusere på vannmiljø og utvikling av vannmiljømetoden
    > Ref. problemer med dagens metode i å regne tap av "areal" for vann

    ### 2. Forbedret datagrunnlag
    **Enten:**

    - Feltkartlegging av Hjartdøla for å forbedre best/worst case scenarione. Feltkartlegging av både påvirkede områder og upåvirkede referanseområder slik at vi kan ekstrapolere "før-tilstanden"

    **Eller:**

    - Fokusere på ett prosjekt med tilstrekkelig kartleggingsdata


    ### 3. Utsette følsomhetsanalyser
    Vente med å gjennomføre følsomhetsanalyser til **usikkerheten** i naturregnskapet er sterkt redusert.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Fra kontrakten: 

    ### Hovedmål
    Vurdere i hvilken grad naturregnskap med Naturpoengmetoden kan benyttes som verktøy i arbeidet med å fremskaffe et **kunnskapsbasert grunnlag** for å utarbeide **prioriterte tiltaksplaner**.

    ### Metodiske utfordringer

    Skagerak Kraft ønsker å få kunnskap om det er hensiktsmessig å anvende Naturpoengmetoden til å etablere naturregnskap for kraftanlegg som er i drift, og hvor det **ikke finnes oppdaterte naturdata** for naturgrunnlag og -tilstand før etablering av kraftanlegget.

    Videre hva som eventuelt må til for å tilpasse metodikken til å etablere et naturregnskap for anlegg i drift.

    ### Følgende formål og spørsmål er sentrale for Skagerak Kraft:

    **Følsomhetsanalyse:**
    - Utføre følsomhetsanalyse på input-faktorene i naturregnskapet
    - Avdekke hva usikkerhet betyr for de **viktigste datadriverne** i Naturpoengmetoden

    **Metodens modenhet:**
    - Vurdere om og i hvilken grad Naturpoengmetoden er moden for å anvende på kraftanlegg både i drift og i planfase
    - Kan hele eller deler av metoden anvendes, eventuelt med hvilke justeringer?

    **Datagrunnlag:**
    - Hva trengs av datagrunnlag for å kunne etablere et naturregnskap for anlegg i drift?
    - Hvilke data er det mulig å fremskaffe – enten ved kartlegging, modellering, eller annen metode?

    ---
    """
    )
    return


if __name__ == "__main__":
    app.run()
