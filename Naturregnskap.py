import marimo

__generated_with = "0.15.0"
app = marimo.App(width="columns")


@app.cell(column=0)
def _():
    import marimo as mo
    import polars as pl
    import io
    import pathlib
    import pytest as pt
    return io, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Input: P.t. syntetisk datasett """)
    return


@app.cell(hide_code=True)
def _(io, pl):
    # Lager utvidet syntetisk datasett med alle nødvendige felt for komplett Naturregnskap

    eksempel_data = """
    delområde;utstrekning_for_inngrep;arealtype;regnskapstema;naturkvalitet_nivå;forvaltningsinteresse_nivå;tapt_utstrekning;resterende_utstrekning;påvirkning;utstrekning_etter_tiltak;mål_naturkvalitet_nivå;forvaltningsinteresse_skapt_nivå;avstand fra inngrep;Tidsperspektiv;vanskelighetsgrad;utstrekning_før_tiltak_offsite
    A;1.5;Verneområder;Økosystemareal;høyeste kvalitet;Vernet;0.5;1.0;Noe forringet;2.0;høyeste kvalitet;Vernet;Innenfor prosjektområdet + buffersone på 2 km;Opptil 5 år;Lav vanskelighetsgrad;1.2
    B;25.3;Funksjonsområder for arter av nasjonal forvaltningsinteresse;Økologiske funksjonsområder;Svært høy kvalitet;VU;5.0;20.3;Forringet;30.0;Svært høy kvalitet;VU;I direkte nærhet til prosjektområde (utenfor buffersone, men innen radius 20 km);5 til 10 år;Middels vanskelighetsgrad;22.0
    C;5.7;Vannforekomster inkl. tilhørende funksjonsområder for vannlevende organismer;Vannforekomster;Moderat kvalitet;Vassdrag med fiskebestander av regional/lokal verdi;1.2;4.5;Ubetydelig endring;8.0;Høy kvalitet;Vassdrag med fiskebestander av regional/lokal verdi;Utenfor prosjektområde, men i samme bioklimatiske sone;10 til 20 år;Høy vanskelighetsgrad;4.8
    """
    delområder_df = pl.read_csv(
        io.StringIO(eksempel_data),
        separator=';',
        schema_overrides={
            'utstrekning_for_inngrep': pl.Float64,
            'tapt_utstrekning': pl.Float64, 
            'resterende_utstrekning': pl.Float64,
            'utstrekning_etter_tiltak': pl.Float64,
            'utstrekning_før_tiltak_offsite': pl.Float64
        }
    )

    delområder_df
    return (delområder_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Ferdige beregninger (output datasett)""")
    return


@app.cell(hide_code=True)
def _(
    calculate_naturpoeng_for_inngrep,
    calculate_naturpoeng_skapt_offsite,
    calculate_naturpoeng_skapt_onsite,
    calculate_naturpoeng_tapt,
    calculate_total_endring,
    naturregnskaps_data_df,
):
    # Appliserer alle beregninger i rekkefølge
    result_df = naturregnskaps_data_df
    result_df = calculate_naturpoeng_for_inngrep(result_df)
    result_df = calculate_naturpoeng_tapt(result_df)  
    result_df = calculate_naturpoeng_skapt_onsite(result_df)
    result_df = calculate_naturpoeng_skapt_offsite(result_df)
    final_result_df = calculate_total_endring(result_df)

    final_result_df
    return


@app.cell(column=1, hide_code=True)
def _(mo):
    mo.md(r"""## Data pipeline""")
    return


@app.cell
def _(pl):
    # Laster alle CSV filer og rydder kolonnene
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

    # Laster risikofaktor og påvirknings tabeller
    avstand_df = (
        pl.read_csv("avstand fra inngrep.csv", separator=";", encoding="utf-8", decimal_comma=True)
        .with_columns([pl.col("avstand fra inngrep").str.strip_chars()])
    )

    pavirkning_df = (
        pl.read_csv("påvirkning.csv", separator=";", encoding="utf-8", decimal_comma=True)
        .with_columns([pl.col("påvirkning").str.strip_chars()])
    )

    tidsperspektiv_df = (
        pl.read_csv("tidsperspektiv.csv", separator=";", encoding="utf-8", decimal_comma=True)
        .with_columns([pl.col("Tidsperspektiv").str.strip_chars()])
    )

    vanskelighetsgrad_df = (
        pl.read_csv("vanskelighetsgrad.csv", separator=";", encoding="utf-8", decimal_comma=True)
        .with_columns([pl.col("vanskelighetsgrad").str.strip_chars()])
    )

    return (
        avstand_df,
        forvaltnings_interesse_df,
        naturkvalitet_df,
        pavirkning_df,
        tidsperspektiv_df,
        vanskelighetsgrad_df,
    )


@app.cell
def _(
    avstand_df,
    delområder_df,
    forvaltnings_interesse_df,
    naturkvalitet_df,
    pavirkning_df,
    pl,
    tidsperspektiv_df,
    vanskelighetsgrad_df,
):
    # Joiner alle relevante data fra CSV filene til hovedtabellen

    # Først joiner vi naturkvalitet data (for både før og mål nivåer)
    joined_df = delområder_df.join(
        naturkvalitet_df,
        on=["arealtype", "regnskapstema", "naturkvalitet_nivå"], 
        how="left"
    )

    # Joiner forvaltningsinteresse (før)
    joined_df = joined_df.join(
        forvaltnings_interesse_df,
        on=["arealtype", "regnskapstema", "forvaltningsinteresse_nivå"],
        how="left"
    )

    # Joiner mål naturkvalitet verdier (med suffix for å skille fra før-verdier)
    mål_naturkvalitet_df = naturkvalitet_df.select([
        "arealtype", "regnskapstema", 
        pl.col("naturkvalitet_nivå").alias("mål_naturkvalitet_nivå"),
        pl.col("naturkvalitet_verdi").alias("mål_naturkvalitet_verdi")
    ])

    joined_df = joined_df.join(
        mål_naturkvalitet_df,
        on=["arealtype", "regnskapstema", "mål_naturkvalitet_nivå"],
        how="left"
    )

    # Joiner forvaltningsinteresse for skapt natur (med suffix)
    forvaltnings_skapt_df = forvaltnings_interesse_df.select([
        "arealtype", "regnskapstema",
        pl.col("forvaltningsinteresse_nivå").alias("forvaltningsinteresse_skapt_nivå"),
        pl.col("forvaltningsinteresse_verdi").alias("forvaltningsinteresse_skapt_verdi")
    ])

    joined_df = joined_df.join(
        forvaltnings_skapt_df,
        on=["arealtype", "regnskapstema", "forvaltningsinteresse_skapt_nivå"],
        how="left"
    )

    # Joiner risikofaktorer og påvirkning
    joined_df = joined_df.join(avstand_df, on="avstand fra inngrep", how="left")
    joined_df = joined_df.join(pavirkning_df, on="påvirkning", how="left")
    joined_df = joined_df.join(tidsperspektiv_df, on="Tidsperspektiv", how="left")
    joined_df = joined_df.join(vanskelighetsgrad_df, on="vanskelighetsgrad", how="left")

    # Den komplette datasettet for alle beregninger
    naturregnskaps_data_df = joined_df

    naturregnskaps_data_df
    return (naturregnskaps_data_df,)


@app.cell(column=2, hide_code=True)
def _(mo):
    mo.md(r"""## Naturregnskap metode""")
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


@app.cell(column=3, hide_code=True)
def _(mo):
    mo.md(r"""## Funksjoner for beregning av naturpoeng""")
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

    return (calculate_naturpoeng_for_inngrep,)


@app.cell
def _(pl):
    def calculate_naturpoeng_tapt(df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate Naturpoeng (tapt) for each row.

        Formula: (Tapt utstrekning × Naturkvalitet før × Forvaltningsinteresse før) +
                (Resterende utstrekning × Naturkvalitet før × Forvaltningsinteresse før × Påvirkningsfaktor)

        Args:
            df: DataFrame with required columns for calculation

        Returns:
            DataFrame with added 'naturpoeng_tapt' column
        """
        return df.with_columns([
            ((pl.col("tapt_utstrekning").cast(pl.Float64) *
              pl.col("naturkvalitet_verdi").cast(pl.Float64) *
              pl.col("forvaltningsinteresse_verdi").cast(pl.Float64)) +
             (pl.col("resterende_utstrekning").cast(pl.Float64) *
              pl.col("naturkvalitet_verdi").cast(pl.Float64) *
              pl.col("forvaltningsinteresse_verdi").cast(pl.Float64) *
              pl.col("påvirkning_verdi").cast(pl.Float64))).alias("naturpoeng_tapt")
        ])

    return (calculate_naturpoeng_tapt,)


@app.cell
def _(pl):
    def calculate_naturpoeng_skapt_onsite(df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate Naturpoeng (skapt onsite) for each row.

        Formula: (Utstrekning etter tiltak × Mål naturkvalitet × Forvaltningsinteresse) ×
                (Risikofaktor avstand × Risikofaktor tid × Risikofaktor vanskelighetsgrad)

        Args:
            df: DataFrame with required columns for calculation

        Returns:
            DataFrame with added 'naturpoeng_skapt_onsite' column
        """
        return df.with_columns([
            (pl.col("utstrekning_etter_tiltak").cast(pl.Float64) *
             pl.col("mål_naturkvalitet_verdi").cast(pl.Float64) *
             pl.col("forvaltningsinteresse_skapt_verdi").cast(pl.Float64) *
             pl.col("risikofaktor_avstand fra inngrep").cast(pl.Float64) *
             pl.col("risikofaktor_tidsperspektiv").cast(pl.Float64) *
             pl.col("risikofaktor_vanskelighetsgrad").cast(pl.Float64)).alias("naturpoeng_skapt_onsite")
        ])

    return (calculate_naturpoeng_skapt_onsite,)


@app.cell
def _(pl):
    def calculate_naturpoeng_skapt_offsite(df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate Naturpoeng (skapt offsite) for each row.

        Formula: {(Utstrekning etter tiltak × Mål naturkvalitet × Forvaltningsinteresse) -
                 (Utstrekning før tiltak × Naturkvalitet før × Forvaltningsinteresse før)} ×
                 (Risikofaktor vanskelighetsgrad × Risikofaktor tid × Risikofaktor avstand)

        Args:
            df: DataFrame with required columns for calculation

        Returns:
            DataFrame with added 'naturpoeng_skapt_offsite' column
        """
        return df.with_columns([
            (((pl.col("utstrekning_etter_tiltak").cast(pl.Float64) *
               pl.col("mål_naturkvalitet_verdi").cast(pl.Float64) *
               pl.col("forvaltningsinteresse_skapt_verdi").cast(pl.Float64)) -
              (pl.col("utstrekning_før_tiltak_offsite").cast(pl.Float64) *
               pl.col("naturkvalitet_verdi").cast(pl.Float64) *
               pl.col("forvaltningsinteresse_verdi").cast(pl.Float64))) *
             pl.col("risikofaktor_vanskelighetsgrad").cast(pl.Float64) *
             pl.col("risikofaktor_tidsperspektiv").cast(pl.Float64) *
             pl.col("risikofaktor_avstand fra inngrep").cast(pl.Float64)).alias("naturpoeng_skapt_offsite")
        ])

    return (calculate_naturpoeng_skapt_offsite,)


@app.cell
def _(pl):
    def calculate_total_endring(df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate total endring summary for the complete dataset.

        Formula: Total endring = Σ(Naturpoeng før) - Σ(Naturpoeng tapt) + Σ(Naturpoeng skapt onsite + offsite)

        Args:
            df: DataFrame with all calculated naturpoeng columns

        Returns:
            DataFrame with summary totals and total_endring
        """
        return df.with_columns([
            pl.col("naturpoeng_for_inngrep").sum().alias("total_naturpoeng_før"),
            pl.col("naturpoeng_tapt").sum().alias("total_naturpoeng_tapt"),
            (pl.col("naturpoeng_skapt_onsite").sum() + pl.col("naturpoeng_skapt_offsite").sum()).alias("total_naturpoeng_skapt"),
            (pl.col("naturpoeng_for_inngrep").sum() - pl.col("naturpoeng_tapt").sum() + 
             pl.col("naturpoeng_skapt_onsite").sum() + pl.col("naturpoeng_skapt_offsite").sum()).alias("total_endring")
        ])

    return (calculate_total_endring,)


@app.cell(column=4, hide_code=True)
def _(mo):
    mo.md(r"""## Test""")
    return


@app.cell
def _(calculate_naturpoeng_for_inngrep, pl):
    def test_naturpoeng_før():
        # Arrange
        test_data = pl.DataFrame({
            "delområde": ["Test_A"],
            "utstrekning_for_inngrep": [10.0],
            "naturkvalitet_verdi": [5.0],
            "forvaltningsinteresse_verdi": [2]
        })

        # Act
        result_test_npfør = calculate_naturpoeng_for_inngrep(test_data) #Når du kjører funksjonen så lager du også den nye kolonnen som er "naturpoeng_for_inngrep" som er sum kolonnen fra denne funksjonen. Så du passer test dataene inn i denne
    
    
        # Assert
        expected_value = 10.0 * 5.0 * 2.0  # = 100
        actual_value = result_test_npfør["naturpoeng_for_inngrep"][0] #[0] sier bare start på kolonnen 0, men her har du bare en kolonne så trengs egentlig ikke. 

        assert abs(actual_value - expected_value) < 0.0001
    


    return


if __name__ == "__main__":
    app.run()
