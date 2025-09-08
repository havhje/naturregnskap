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
    return Image, alt, mo, pl


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
def _(mo):
    mo.md(
        r"""
    ## Naturpoeng før inngrep


    $$\text{Naturpoeng (før)} = A_{t0} \times Nk_{t0} \times Nf_{t0}$$

    **Hvor:**

    $A_{t0}$ = Areal (daa) eller lineær vannforekomst (m) før tiltak

    $Nk_{t0}$ = Naturkvalitet før tiltak

    $Nf_{t0}$ = Forvaltningsinteresse før tiltak

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
def _(Image, mo):

    # Load and display the image
    image_path = "/Users/havardhjermstad-sollerud/Documents/Kodeprosjekter/marimo/naturregnskap/Hjartdøla/oversikthjartdøla.png"
    img = Image.open(image_path)


    mo.vstack([
        mo.hstack([mo.md("### Oversikt Hjartdøla")]),
        mo.hstack([mo.image(img)]),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    hjartdola_df = mo.sql(
        f"""
        SELECT * FROM '/Users/havardhjermstad-sollerud/Documents/Kodeprosjekter/marimo/naturregnskap/Hjartdøla/hjartdøla.csv';
        """,
        output=False
    )
    return (hjartdola_df,)


@app.cell
def _(mo):
    mo.md(
        r"""
    # Lag den under selv, men diu må ha med før/etter. Lag fire "celler" og bruk mo.hstack til å stacke de. Under der så kan du ha formelen. 

    Få med rangen/spennet til alle verdiene. og forklaringen på hvorfor verdien er satt som den er. 
    """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Konstante verdier per scenario

    *Lik for alle delområder - kun areal/utstrekning som endres*

    ### Best Case Scenario
    - **Naturkvalitet (Nk)**: 2.2 *(mulig verdiområde - sjekkkk!!!: 0-4)*
    - **Forvaltningsinteresse (Nf)**: 3.0 *(mulig verdiområde: 0-4)*
    - **Konstant produkt**: Nk × Nf = 2.2 × 3.0 = **6.6**

    ### Worst Case Scenario  
    - **Naturkvalitet (Nk)**: 1.0 *(mulig verdiområde: 0-4)*
    - **Forvaltningsinteresse (Nf)**: 1.0 *(mulig verdiområde: 0-4)*
    - **Konstant produkt**: Nk × Nf = 1.0 × 1.0 = **1.0**

    ---

    ### Variabel per delområde
    - **Utstrekning (U)**: Varierer per delområde (meter)

    ### Forenklet beregning

    I praksis bruker vi følgende formel:

    $$\text{Naturpoeng}_{\text{delområde}} = U_{\text{delområde}} \times (Nk \times Nf)_{\text{scenario}}$$



    """
    )
    return


@app.cell(hide_code=True)
def _(alt, hjartdola_df, mo, pl):
    # Prepare data for visualization
    _chart_data = hjartdola_df.with_columns(
        pl.concat_str([
            pl.col("før/etter"),
            pl.lit(" - "),
            pl.col("best_case/worst_case")
        ]).alias("scenario")
    ).select([
        "delområde",
        "scenario", 
        "før/etter",
        "best_case/worst_case",
        "naturpoeng_for_inngrep"
    ])

    # Create interactive Altair chart
    _base = alt.Chart(_chart_data.to_pandas()).encode(
        y=alt.Y('delområde:N', title='Delområde', sort=None),
        x=alt.X('naturpoeng_for_inngrep:Q', title='Naturpoeng'),
        color=alt.Color('delområde:N', legend=None),
        opacity=alt.Opacity(
            'før/etter:N',
            scale=alt.Scale(domain=['Før', 'Etter'], range=[1, 0.6]),
            legend=alt.Legend(title='Tidspunkt', titleFontSize=14, labelFontSize=12)
        ),
        strokeDash=alt.StrokeDash(
            'best_case/worst_case:N',
            scale=alt.Scale(domain=['Best case', 'Worst case'], range=[[1, 0], [5, 5]]),
            legend=alt.Legend(title='Scenario', titleFontSize=14, labelFontSize=12)
        ),
        tooltip=['delområde:N', 'scenario:N', 'naturpoeng_for_inngrep:Q']
    )

    _bars = _base.mark_bar(
        stroke='white',
        strokeWidth=3,
        height=25
    ).encode(
        y=alt.Y('scenario:N', title=''),
        row=alt.Row('delområde:N', header=alt.Header(labelAngle=0, labelAlign='left', title='', labelFontSize=13))
    )

    _chart = _bars.properties(
        width=2000,
        height=150,
        title=alt.TitleParams(
            text='Naturpoeng per delområde - Før/Etter & Best/Worst Case',
            fontSize=16,
            fontWeight='normal'
        )
    ).configure_facet(
        spacing=8
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    ).configure_legend(
        symbolSize=120,
        symbolStrokeWidth=2,
        padding=10,
        columnPadding=15,
        rowPadding=5
    ).interactive()

    mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Slide med forslag til videre arbeid og utvikling

    Fokusere på akvatisk
    Feltkartlegging
    Eller for områder som er godt kartlagt
    Videreutvikle beregningsmetodikk for vann
    """
    )
    return


if __name__ == "__main__":
    app.run()
