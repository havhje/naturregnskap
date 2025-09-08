import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    from PIL import Image
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
    mo.md(r""" """)
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
        """
    )
    return (hjartdola_df,)


@app.cell
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
            legend=alt.Legend(title='Tidspunkt')
        ),
        strokeDash=alt.StrokeDash(
            'best_case/worst_case:N',
            scale=alt.Scale(domain=['Best case', 'Worst case'], range=[[1, 0], [5, 5]]),
            legend=alt.Legend(title='Scenario')
        ),
        tooltip=['delområde:N', 'scenario:N', 'naturpoeng_for_inngrep:Q']
    )

    _bars = _base.mark_bar(
        stroke='white',
        strokeWidth=2,
        height=15
    ).encode(
        y=alt.Y('scenario:N', title=''),
        row=alt.Row('delområde:N', header=alt.Header(labelAngle=0, labelAlign='left', title=''))
    )

    _chart = _bars.properties(
        width=600,
        height=50,
        title='Naturpoeng per delområde - Før/Etter & Best/Worst Case'
    ).configure_facet(
        spacing=5
    ).configure_axis(
        labelFontSize=11,
        titleFontSize=12
    ).interactive()

    mo.ui.altair_chart(_chart)
    return


if __name__ == "__main__":
    app.run()
