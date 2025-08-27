import marimo

__generated_with = "0.15.0"
app = marimo.App(width="columns")


@app.cell(column=0, hide_code=True)
def _(mo):
    mo.md(r"""# Naturregnskap metode""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Inndeling av naturmangfoldet""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Økosystemareal""")
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### 1. Naturpoeng (før inngrep)
    $$
    \text{Naturpoeng}_{\text{før}} = A_{t0} \times Nk_{t0} \times Nf_{t0}
    $$

    ### 2. Naturpoeng (tapt)
    $$
    \text{Naturpoeng (tapt)} = (A_{\text{tapt}} \times Nk_{t0} \times Nf_{t0}) + (A_{\text{rest}} \times Nk_{t0} \times Nf_{t0}) \times P
    $$

    ### 3-1. Naturpoeng (skapt natur)
    $$
    \text{Naturpoeng (skapt)} = A_{t1} \times Nk_{t1} \times Nf_{t1} \times R_{\text{avst}} \times R_{\text{tid}} \times R_{\text{van}}
    $$

    ### 3-2. Naturpoeng (skapt natur off-site)
    $$
    \text{Naturpoeng (off-site)} = \{(A_{t1} \times Nk_{t1} \times Nf_{t1}) - (A_{t0} \times Nk_{t0} \times Nf_{t0})\} \times R_{\text{van}} \times R_{\text{tid}} \times R_{\text{avst}}
    $$

    ### Total endring av Naturpoeng
    $$
    \text{Total endring} = \sum \text{Naturpoeng}_{\text{før}} - \sum \text{Naturpoeng}_{\text{tapt}} + \sum \text{Naturpoeng}_{\text{skapt}}
    $$
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### 1. Naturpoeng (før inngrep)
    $$
    \text{Naturpoeng (før inngrep)} = (\text{Utstrekning før inngrep}) \times (\text{Naturkvalitet før inngrep}) \times (\text{Forvaltningsinteresse før inngrep})
    $$

    ### 2. Naturpoeng (tapt)
    $$
    \begin{aligned}
    \text{Naturpoeng (tapt)} = & \; (\text{Tapt utstrekning} \times \text{Naturkvalitet før inngrep} \times \text{Forvaltningsinteresse før inngrep}) \\
    & + (\text{Resterende utstrekning} \times \text{Naturkvalitet før inngrep} \times \text{Forvaltningsinteresse før inngrep}) \times \text{Påvirkningsfaktor}
    \end{aligned}
    $$

    ### 3-1. Naturpoeng (skapt natur)
    $$
    \begin{aligned}
    \text{Naturpoeng (skapt)} = & \; (\text{Utstrekning etter tiltak}) \times (\text{Mål for naturkvalitet}) \times (\text{Forvaltningsinteresse}) \\
    & \times (\text{Risikofaktor avstand}) \times (\text{Risikofaktor tid}) \times (\text{Risikofaktor vanskelighetsgrad})
    \end{aligned}
    $$

    ### 3-2. Naturpoeng (skapt natur off-site)
    $$
    \begin{aligned}
    \text{Naturpoeng (off-site)} = & \; \{(\text{Utstrekning etter tiltak} \times \text{Mål for naturkvalitet} \times \text{Forvaltningsinteresse}) \\
    & - (\text{Utstrekning før tiltak} \times \text{Naturkvalitet før tiltak} \times \text{Forvaltningsinteresse før tiltak})\} \\
    & \times \text{Risikofaktor vanskelighetsgrad} \times \text{Risikofaktor tid} \times \text{Risikofaktor avstand}
    \end{aligned}
    $$

    ### Total endring av Naturpoeng
    $$
    \text{Total endring} = (\sum \text{Naturpoeng før}) - (\sum \text{Naturpoeng tapt}) + (\sum \text{Naturpoeng skapt})
    $$
    """
    )
    return


@app.cell(column=1)
def _():
    import marimo as mo
    import polars as pl
    import io
    import pathlib
    return io, mo, pl


@app.cell(hide_code=True)
def _(io, pl):
    # Lager syntetisk datasett i en polars df

    eksempel_data = """
    delområde;utstrekning_for_inngrep;arealtype;regnskapstema;naturkvalitet_nivå;forvaltningsinteresse_nivå
    A;1.5;Verneområder;Økosystemareal;høyeste kvalitet;Vernet
    B;25.3;Funksjonsområder for arter av nasjonal forvaltningsinteresse;Økologiske funksjonsområder;Svært høy kvalitet;VU
    C;5.7;Vannforekomster inkl. tilhørende funksjonsområder for vannlevende organismer;Vannforekomster;Moderat kvalitet;Vassdrag med fiskebestander av regional/lokal verdi
    """
    delområder_df = pl.read_csv(
        io.StringIO(eksempel_data),
        separator=';',
        schema_overrides={'utstrekning_for_inngrep': pl.Float64}
    )

    delområder_df
    return (delområder_df,)


@app.cell(hide_code=True)
def _(delområder_df, pl):
    #Laster csv til df og rydder kolonnene 
    forvaltnings_interesse_df = (
        pl.read_csv("forvaltningsinteresse.csv", separator=";", encoding="utf-8", decimal_comma=True)
        .with_columns([
            pl.col("arealtype").str.strip_chars(),
            pl.col("regnskapstema").str.strip_chars(),
            pl.col("forvaltningsinteresse_nivå").str.strip_chars()
        ])
    )

    naturkvalitet_df = (
        pl.read_csv("naturkvalitet.csv", separator=";", encoding="utf-8", decimal_comma=True)
        .with_columns([
            pl.col("arealtype").str.strip_chars(),
            pl.col("regnskapstema").str.strip_chars(),
            pl.col("naturkvalitet_nivå").str.strip_chars()
        ])
    )

    #joiner relevante data fra csv filene til hovedtabellen (dvs. den med delområder)
    joined_df = delområder_df.join(
        naturkvalitet_df,
        on=["arealtype", "regnskapstema", "naturkvalitet_nivå"], 
        how="left"
    )

    naturregnskaps_data_df = joined_df.join(
        forvaltnings_interesse_df,
        on=["arealtype", "regnskapstema", "forvaltningsinteresse_nivå"],
        how="left"
    )

    return


@app.cell(column=2, hide_code=True)
def _(mo):
    mo.md(r"""## Utility functions""")
    return


@app.cell
def _(pl):
    # naturpoeng før funksjon

    def calculate_naturpoeng_for_inngrep(df: pl.DataFrame) -> pl.DataFrame:
          """
          Calculate Naturpoeng (før inngrep) for each row.

          Formula: utstrekning_for_inngrep × naturkvalitet_verdi × forvaltningsinteresse_verdi

          Args:
              df: DataFrame with columns 'utstrekning_for_inngrep', 'naturkvalitet_verdi', 'forvaltningsinteresse_verdi'

          Returns:
              DataFrame with added 'naturpoeng_for_inngrep' column
          """
          return df.with_columns([
              (pl.col("utstrekning_for_inngrep").cast(pl.Float64) *
               pl.col("naturkvalitet_verdi").cast(pl.Float64) *
               pl.col("forvaltningsinteresse_verdi").cast(pl.Float64)).alias("naturpoeng_for_inngrep")
          ])
    return


if __name__ == "__main__":
    app.run()
