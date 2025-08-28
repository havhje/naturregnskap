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
    return mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Input: P.t. syntetisk datasett""")
    return


@app.cell
def _(pl):
    # Create synthetic dataset using dictionaries - much more readable and easy to modify
    delområder_data = [
        {
            "delområde": "A",
            "utstrekning_for_inngrep": 1.5,
            "arealtype": "Verneområder",
            "regnskapstema": "Økosystemareal",
            "naturkvalitet_nivå": "høyeste kvalitet",
            "forvaltningsinteresse_nivå": "Vernet",
            "tapt_utstrekning": 0.5,
            "resterende_utstrekning": 1.0,
            "påvirkning": "Noe forringet",
            "istandsatt_økosystemareal": 2.0,
            "mål_naturkvalitet_nivå": "høyeste kvalitet",
            "mål_forvaltningsinteresse_nivå": "Vernet",
            "avstand fra inngrep": "Innenfor prosjektområdet + buffersone på 2 km",
            "Tidsperspektiv": "Opptil 5 år",
            "vanskelighetsgrad": "Lav vanskelighetsgrad",
            "utstrekning_før_tiltak_offsite": 1.2
        },
        {
            "delområde": "B",
            "utstrekning_for_inngrep": 25.3,
            "arealtype": "Funksjonsområder for arter av nasjonal forvaltningsinteresse",
            "regnskapstema": "Økologiske funksjonsområder",
            "naturkvalitet_nivå": "Svært høy kvalitet",
            "forvaltningsinteresse_nivå": "VU",
            "tapt_utstrekning": 5.0,
            "resterende_utstrekning": 20.3,
            "påvirkning": "Forringet",
            "istandsatt_økosystemareal": 30.0,
            "mål_naturkvalitet_nivå": "Svært høy kvalitet",
            "mål_forvaltningsinteresse_nivå": "VU",
            "avstand fra inngrep": "I direkte nærhet til prosjektområde (utenfor buffersone, men innen radius 20 km)",
            "Tidsperspektiv": "5 til 10 år",
            "vanskelighetsgrad": "Middels vanskelighetsgrad",
            "utstrekning_før_tiltak_offsite": 22.0
        },
        {
            "delområde": "C",
            "utstrekning_for_inngrep": 5.7,
            "arealtype": "Vannforekomster inkl. tilhørende funksjonsområder for vannlevende organismer",
            "regnskapstema": "Vannforekomster",
            "naturkvalitet_nivå": "Moderat kvalitet",
            "forvaltningsinteresse_nivå": "Vassdrag med fiskebestander av regional/lokal verdi",
            "tapt_utstrekning": 1.2,
            "resterende_utstrekning": 4.5,
            "påvirkning": "Ubetydelig endring",
            "istandsatt_økosystemareal": 8.0,
            "mål_naturkvalitet_nivå": "Høy kvalitet",
            "mål_forvaltningsinteresse_nivå": "Vassdrag med fiskebestander av regional/lokal verdi",
            "avstand fra inngrep": "Utenfor prosjektområde, men i samme bioklimatiske sone",
            "Tidsperspektiv": "10 til 20 år",
            "vanskelighetsgrad": "Høy vanskelighetsgrad",
            "utstrekning_før_tiltak_offsite": 4.8
        }
    ]

    # Convert to Polars DataFrame
    delområder_df = pl.DataFrame(delområder_data)
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
        pl.col("forvaltningsinteresse_nivå").alias("mål_forvaltningsinteresse_nivå"),
        pl.col("forvaltningsinteresse_verdi").alias("mål_forvaltningsinteresse_verdi")
    ])

    joined_df = joined_df.join(
        forvaltnings_skapt_df,
        on=["arealtype", "regnskapstema", "mål_forvaltningsinteresse_nivå"],
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

        Formula: (Tapt utstrekning × Naturkvalitet × Forvaltningsinteresse) +
                (Resterende utstrekning × Naturkvalitet × Forvaltningsinteresse) × Påvirkningsfaktor)

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

        Formula: (Utstrekning etter tiltak × Mål naturkvalitet × Mål forvaltningsinteresse) ×
                (Risikofaktor avstand × Risikofaktor tid × Risikofaktor vanskelighetsgrad)

        Args:
            df: DataFrame with required columns for calculation

        Returns:
            DataFrame with added 'naturpoeng_skapt_onsite' column
        """
        return df.with_columns([
            (pl.col("istandsatt_økosystemareal").cast(pl.Float64) *
             pl.col("mål_naturkvalitet_verdi").cast(pl.Float64) *
             pl.col("mål_forvaltningsinteresse_verdi").cast(pl.Float64) *
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

        Formula: {(Utstrekning etter tiltak × Mål naturkvalitet × Mål forvaltningsinteresse) -
                 (Utstrekning før tiltak × Naturkvalitet × Forvaltningsinteresse)} ×
                 (Risikofaktor vanskelighetsgrad × Risikofaktor tid × Risikofaktor avstand)

                 # NP skapt offsite = (Poeng restaurert areal - Poeng det som var der fra før) * Risikofaktorer

        Args:
            df: DataFrame with required columns for calculation

        Returns:
            DataFrame with added 'naturpoeng_skapt_offsite' column
        """
        return df.with_columns([
            (((pl.col("istandsatt_økosystemareal").cast(pl.Float64) *
               pl.col("mål_naturkvalitet_verdi").cast(pl.Float64) *
               pl.col("mål_forvaltningsinteresse_verdi").cast(pl.Float64)) -
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


@app.cell
def _(calculate_naturpoeng_tapt, pl):
    def test_naturpoeng_tapt():
        # Arrange
        test_data1 = pl.DataFrame({
            "delområde": ["Test_A"],
            "tapt_utstrekning": [6.0],
            "naturkvalitet_verdi": [5.0],
            "forvaltningsinteresse_verdi": [2],
            "resterende_utstrekning": [4.0],
            "påvirkning_verdi": [5.0]
        
        })

        # Act
        result_test_nptapt = calculate_naturpoeng_tapt(test_data1) 
    
        # Assert
        expected_value = (6 * 5 * 2) + (4 * 5 * 2) * 5  # = 250
        actual_value = result_test_nptapt["naturpoeng_tapt"][0] #[0] sier bare start på kolonnen 0, men her har du bare en kolonne så trengs egentlig ikke. 

        assert abs(actual_value - expected_value) < 0.0001



    return


@app.cell
def _(calculate_naturpoeng_skapt_onsite, pl):
    def test_naturpoeng_skapt_onsite():
        # Arrange
        test_data = pl.DataFrame({
            "delområde": ["Test_A"],
            "istandsatt_økosystemareal": [8.0],
            "mål_naturkvalitet_verdi": [4.0],
            "mål_forvaltningsinteresse_verdi": [3.0],
            "risikofaktor_avstand fra inngrep": [0.9],
            "risikofaktor_tidsperspektiv": [0.8],
            "risikofaktor_vanskelighetsgrad": [0.7]
        })

        # Act
        result_test_onsite = calculate_naturpoeng_skapt_onsite(test_data)
    
        # Assert
        expected_value = (8.0 * 4.0 * 3.0) * (0.9 * 0.8 * 0.7)  # = 96 * 0.504 = 48.384
        actual_value = result_test_onsite["naturpoeng_skapt_onsite"][0]

        assert abs(actual_value - expected_value) < 0.0001
    return


@app.cell
def _(calculate_naturpoeng_skapt_offsite, pl):
    def test_naturpoeng_skapt_offsite():
        # Arrange
        test_data = pl.DataFrame({
            "delområde": ["Test_A"],
            "istandsatt_økosystemareal": [10.0],
            "mål_naturkvalitet_verdi": [5.0],
            "mål_forvaltningsinteresse_verdi": [3.0],
            "utstrekning_før_tiltak_offsite": [6.0],
            "naturkvalitet_verdi": [4.0],
            "forvaltningsinteresse_verdi": [2.0],
            "risikofaktor_vanskelighetsgrad": [0.8],
            "risikofaktor_tidsperspektiv": [0.7],
            "risikofaktor_avstand fra inngrep": [0.9]
        })

        # Act
        result_test_offsite = calculate_naturpoeng_skapt_offsite(test_data)
    
        # Assert
        expected_value = ((10.0 * 5.0 * 3.0) - (6.0 * 4.0 * 2.0)) * (0.8 * 0.7 * 0.9)  # = (150 - 48) * 0.504 = 102 * 0.504 = 51.408
        actual_value = result_test_offsite["naturpoeng_skapt_offsite"][0]

        assert abs(actual_value - expected_value) < 0.0001
    return


@app.cell
def _(calculate_total_endring, pl):
    def test_calculate_total_endring():
        # Arrange
        test_data = pl.DataFrame({
            "delområde": ["Test_A"],
            "naturpoeng_for_inngrep": [100.0],
            "naturpoeng_tapt": [50.0],
            "naturpoeng_skapt_onsite": [30.0],
            "naturpoeng_skapt_offsite": [20.0]
        })

        # Act
        result_test_total = calculate_total_endring(test_data)
    
        # Assert
        expected_value = 100.0 - 50.0 + 30.0 + 20.0  # = 100
        actual_value = result_test_total["total_endring"][0]
    
        assert abs(actual_value - expected_value) < 0.0001
    return


if __name__ == "__main__":
    app.run()
