import marimo

__generated_with = "0.15.0"
app = marimo.App(width="columns")


@app.cell(column=0, hide_code=True)
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


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell(column=1)
def _():
    import marimo as mo
    import polars as pl
    import io
    import pathlib
    import pytest as pt
    import duckdb
    return duckdb, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Input: P.t. syntetisk datasett""")
    return


@app.cell(hide_code=True)
def _(pl):
    # Create synthetic dataset using dictionaries - much more readable and easy to modify
    delområder_data = [
        {
            "delområde": "A", #Eks med både tap og restaurering
            "utstrekning_for_inngrep": 100,
            "arealtype": "Øvrig natur",
            "regnskapstema": "Økosystemareal",
            "naturkvalitet_nivå": "Svært høy lokalitetskvalitet",
            "forvaltningsinteresse_nivå": "Ingen",
        
            "tapt_utstrekning": 30,
            "resterende_utstrekning": 70.0,
            "påvirkning": "Noe forringet",
        
            "istandsatt_økosystemareal": 10.0,
            "mål_naturkvalitet_nivå": "Høy kvalitet",
            "mål_forvaltningsinteresse_nivå": "Ingen",
            "avstand fra inngrep": "Innenfor prosjektområdet + buffersone på 2 km",
            "Tidsperspektiv": "Opptil 5 år",
            "vanskelighetsgrad": "Lav vanskelighetsgrad",
        },
        {
            "delområde": "B",#Eks med bare restaurering onsite 
            "istandsatt_økosystemareal": 40.0,
            "mål_naturkvalitet_nivå": "Høy lokalitetskvalitet",
            "mål_forvaltningsinteresse_nivå": "Ingen",
            "avstand fra inngrep": "I direkte nærhet til prosjektområde (utenfor buffersone, men innen radius 20 km)",
            "Tidsperspektiv": "5 til 10 år",
            "vanskelighetsgrad": "Middels vanskelighetsgrad",
            "utstrekning_før_tiltak_offsite": 22.0  # Added missing comma here
        },
        {
            "delområde": "C",#Eks med bare restaurering offsite 
            "utstrekning_før_tiltak_offsite": 62,  # Added missing comma here
            "arealtype": "Øvrig natur",
            "regnskapstema": "Økosystemareal",
            "naturkvalitet_nivå": "Moderat lokalitetskvalitet",
            "forvaltningsinteresse_nivå": "Ingen",

            "istandsatt_økosystemareal": 10.0,
            "mål_naturkvalitet_nivå": "Høy lokalitetskvalitet",
            "mål_forvaltningsinteresse_nivå": "Ingen",
            
            "avstand fra inngrep": "Innenfor prosjektområdet + buffersone på 2 km",
            "Tidsperspektiv": "Opptil 5 år",
            "vanskelighetsgrad": "Lav vanskelighetsgrad",
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


@app.cell(column=2, hide_code=True)
def _(mo):
    mo.md(r"""## Data pipeline""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- Load all CSV files into DuckDB tables
        CREATE OR REPLACE TABLE forvaltningsinteresse AS 
        SELECT * FROM read_csv_auto('forvaltningsinteresse.csv', 
            delim=';', header=true, decimal_separator=',');

        CREATE OR REPLACE TABLE naturkvalitet AS 
        SELECT * FROM read_csv_auto('naturkvalitet.csv', 
            delim=';', header=true, decimal_separator=',');

        CREATE OR REPLACE TABLE avstand AS 
        SELECT * FROM read_csv_auto('avstand fra inngrep.csv', 
            delim=';', header=true, decimal_separator=',');

        CREATE OR REPLACE TABLE pavirkning AS 
        SELECT * FROM read_csv_auto('påvirkning.csv', 
            delim=';', header=true, decimal_separator=',');

        CREATE OR REPLACE TABLE tidsperspektiv AS 
        SELECT * FROM read_csv_auto('tidsperspektiv.csv', 
            delim=';', header=true, decimal_separator=',');

        CREATE OR REPLACE TABLE vanskelighetsgrad AS 
        SELECT * FROM read_csv_auto('vanskelighetsgrad.csv', 
            delim=';', header=true, decimal_separator=',');
        """
    )
    return avstand, forvaltningsinteresse, naturkvalitet, pavirkning


@app.cell
def _(forvaltningsinteresse, mo, naturkvalitet):
    _df = mo.sql(
        f"""
        -- Create cleaned dataframes with TRIM applied
        CREATE OR REPLACE VIEW forvaltnings_interesse_clean AS
        SELECT 
            TRIM(arealtype) as arealtype,
            TRIM(regnskapstema) as regnskapstema,
            TRIM(forvaltningsinteresse_nivå) as forvaltningsinteresse_nivå,
            forvaltningsinteresse_verdi
        FROM forvaltningsinteresse;

        CREATE OR REPLACE VIEW naturkvalitet_clean AS
        SELECT 
            TRIM(arealtype) as arealtype,
            TRIM(regnskapstema) as regnskapstema,
            TRIM(naturkvalitet_nivå) as naturkvalitet_nivå,
            naturkvalitet_verdi,
            naturkvalitet_definisjon
        FROM naturkvalitet;
        """
    )
    return (forvaltnings_interesse_clean,)


@app.cell
def _(
    avstand,
    duckdb,
    forvaltnings_interesse_clean,
    naturkvalitet_clean,
    pavirkning,
    tidsperspektiv,
    vanskelighetsgrad,
):
    # Access the DuckDB tables as Polars dataframes
    forvaltnings_interesse_df = duckdb.sql("SELECT * FROM forvaltnings_interesse_clean").pl()
    naturkvalitet_df = duckdb.sql("SELECT * FROM naturkvalitet_clean").pl()
    avstand_df = duckdb.sql("SELECT * FROM avstand WHERE \"avstand fra inngrep\" IS NOT NULL").pl()
    pavirkning_df = duckdb.sql("SELECT * FROM pavirkning").pl()
    tidsperspektiv_df = duckdb.sql("SELECT * FROM tidsperspektiv").pl()
    vanskelighetsgrad_df = duckdb.sql("SELECT * FROM vanskelighetsgrad").pl()
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

    # Den komplette datasettet for alle beregninger, hvor alle manglende verdier = 0 fra input datasettet
    naturregnskaps_data_df = joined_df.with_columns([
        pl.col(col).fill_null(0) 
        for col in joined_df.columns 
        if joined_df[col].dtype in [pl.Float64, pl.Int64, pl.Float32, pl.Int32]
    ])

    naturregnskaps_data_df
    return (naturregnskaps_data_df,)


@app.cell
def _(delområder_df, naturkvalitet_df, pl):
    # Check if your joins are working
    diagnostic_df = delområder_df.filter(pl.col("delområde") == "A").join(
        naturkvalitet_df,
        on=["arealtype", "regnskapstema", "naturkvalitet_nivå"],
        how="left"
    )

    print("After join - naturkvalitet_verdi:", diagnostic_df["naturkvalitet_verdi"][0])
    print("Is it None?", diagnostic_df["naturkvalitet_verdi"][0] is None)
    return


@app.cell
def _():
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
        Calculate total endring for each row.

        Formula: Total endring = Naturpoeng før - Naturpoeng tapt + Naturpoeng skapt onsite + Naturpoeng skapt offsite

        Args:
            df: DataFrame with all calculated naturpoeng columns

        Returns:
            DataFrame with all original columns plus total_endring per row
        """
        return df.with_columns([
            (pl.col("naturpoeng_for_inngrep").fill_null(0) -
             pl.col("naturpoeng_tapt").fill_null(0) +
             pl.col("naturpoeng_skapt_onsite").fill_null(0) + 
             pl.col("naturpoeng_skapt_offsite").fill_null(0)).alias("total_endring")
        ])
    return (calculate_total_endring,)


@app.cell(column=4, hide_code=True)
def _(mo):
    mo.md(r"""## Test""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Tester datapipeline""")
    return


@app.cell(hide_code=True)
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
    def test_validate_input_data_combinations():
        """
        Test that all combinations in delområder_df exist in the lookup tables.
        This catches invalid synthetic data early.
        """
        invalid_combinations = []
    
        # Check naturkvalitet combinations
        for row in delområder_df.iter_rows(named=True):
            delområde = row["delområde"]
        
            # Skip rows without naturkvalitet data
            if row["arealtype"] is None or row["regnskapstema"] is None or row["naturkvalitet_nivå"] is None:
                continue
            
            # Check if combination exists in naturkvalitet_df
            matching = naturkvalitet_df.filter(
                (pl.col("arealtype") == row["arealtype"]) &
                (pl.col("regnskapstema") == row["regnskapstema"]) &
                (pl.col("naturkvalitet_nivå") == row["naturkvalitet_nivå"])
            )
        
            if len(matching) == 0:
                invalid_combinations.append({
                    "delområde": delområde,
                    "issue": "naturkvalitet combination not found",
                    "arealtype": row["arealtype"],
                    "regnskapstema": row["regnskapstema"],
                    "naturkvalitet_nivå": row["naturkvalitet_nivå"]
                })
    
        # Check forvaltningsinteresse combinations
        for row in delområder_df.iter_rows(named=True):
            delområde = row["delområde"]
        
            if row["arealtype"] is None or row["regnskapstema"] is None or row["forvaltningsinteresse_nivå"] is None:
                continue
            
            matching = forvaltnings_interesse_df.filter(
                (pl.col("arealtype") == row["arealtype"]) &
                (pl.col("regnskapstema") == row["regnskapstema"]) &
                (pl.col("forvaltningsinteresse_nivå") == row["forvaltningsinteresse_nivå"])
            )
        
            if len(matching) == 0:
                invalid_combinations.append({
                    "delområde": delområde,
                    "issue": "forvaltningsinteresse combination not found",
                    "arealtype": row["arealtype"],
                    "regnskapstema": row["regnskapstema"],
                    "forvaltningsinteresse_nivå": row["forvaltningsinteresse_nivå"]
                })
    
        # Check risikofaktor values
        risk_checks = [
            ("avstand fra inngrep", avstand_df),
            ("påvirkning", pavirkning_df),
            ("Tidsperspektiv", tidsperspektiv_df),
            ("vanskelighetsgrad", vanskelighetsgrad_df)
        ]
    
        for column_name, lookup_df in risk_checks:
            for value in delområder_df[column_name].drop_nulls().unique():
                if len(lookup_df.filter(pl.col(column_name) == value)) == 0:
                    invalid_combinations.append({
                        "issue": f"{column_name} value not found in lookup table",
                        "value": value
                    })
    
        # Assert and provide helpful error message
        if invalid_combinations:
            error_msg = "\n\nINVALID DATA COMBINATIONS FOUND:\n"
            error_msg += "=" * 50 + "\n"
            for invalid in invalid_combinations:
                error_msg += f"\n{invalid}\n"
            error_msg += "\n" + "=" * 50
            error_msg += "\nFix your synthetic data to use valid combinations from the CSV files."
        
            assert False, error_msg
    return


@app.cell(hide_code=True)
def _(naturregnskaps_data_df):
    def test_joined_data_has_required_values():
        """
        Test that after joins, all required calculation fields have values.
        This ensures the pipeline produces valid data for calculations.
        """
        # Run the actual pipeline
        result = naturregnskaps_data_df
    
        missing_values = []
    
        # Check each row for missing critical values
        for row_idx, row in enumerate(result.iter_rows(named=True)):
            delområde = row["delområde"]
        
            # Check values needed for "før inngrep" calculation
            if row["utstrekning_for_inngrep"] is not None:
                if row["naturkvalitet_verdi"] is None:
                    missing_values.append(f"Row {row_idx} (delområde {delområde}): Missing naturkvalitet_verdi")
                if row["forvaltningsinteresse_verdi"] is None:
                    missing_values.append(f"Row {row_idx} (delområde {delområde}): Missing forvaltningsinteresse_verdi")
        
            # Check values needed for "tapt" calculation
            if row["tapt_utstrekning"] is not None:
                if row["påvirkning_verdi"] is None:
                    missing_values.append(f"Row {row_idx} (delområde {delområde}): Missing påvirkning_verdi")
        
            # Check values needed for "skapt" calculations
            if row["istandsatt_økosystemareal"] is not None:
                required_risk_factors = [
                    "risikofaktor_avstand fra inngrep",
                    "risikofaktor_tidsperspektiv", 
                    "risikofaktor_vanskelighetsgrad"
                ]
                for factor in required_risk_factors:
                    if row[factor] is None:
                        missing_values.append(f"Row {row_idx} (delområde {delområde}): Missing {factor}")
    
        # Assert with helpful message
        if missing_values:
            error_msg = "\n\nMISSING VALUES AFTER JOINS:\n"
            error_msg += "=" * 50 + "\n"
            for missing in missing_values:
                error_msg += missing + "\n"
            error_msg += "=" * 50
            error_msg += "\n\nThis means your joins are failing. Check that:"
            error_msg += "\n1. The combinations in your input data exist in CSV files"
            error_msg += "\n2. There are no whitespace/encoding issues"
            error_msg += "\n3. Column names match exactly"
        
            assert False, error_msg
    return


@app.cell(hide_code=True)
def _(
    calculate_naturpoeng_for_inngrep,
    calculate_naturpoeng_skapt_offsite,
    calculate_naturpoeng_skapt_onsite,
    calculate_naturpoeng_tapt,
    calculate_total_endring,
    naturregnskaps_data_df,
    pl,
):
    def test_calculations_produce_non_null_results():
        """
        Integration test that verifies the full pipeline produces valid results.
        """
        # Run full pipeline
        result_df = naturregnskaps_data_df
        result_df = calculate_naturpoeng_for_inngrep(result_df)
        result_df = calculate_naturpoeng_tapt(result_df)
        result_df = calculate_naturpoeng_skapt_onsite(result_df)
        result_df = calculate_naturpoeng_skapt_offsite(result_df)
        final_result = calculate_total_endring(result_df)
    
        # Check that at least some calculations produced non-null, non-zero values
        # Use DataFrame filtering instead of Series filtering
        has_før = final_result.filter(
            (pl.col("naturpoeng_for_inngrep").is_not_null()) & 
            (pl.col("naturpoeng_for_inngrep") != 0)
        ).height > 0
    
        has_tapt = final_result.filter(
            (pl.col("naturpoeng_tapt").is_not_null()) & 
            (pl.col("naturpoeng_tapt") != 0)
        ).height > 0
    
        has_skapt_onsite = final_result.filter(
            (pl.col("naturpoeng_skapt_onsite").is_not_null()) & 
            (pl.col("naturpoeng_skapt_onsite") != 0)
        ).height > 0
    
        has_skapt_offsite = final_result.filter(
            (pl.col("naturpoeng_skapt_offsite").is_not_null()) & 
            (pl.col("naturpoeng_skapt_offsite") != 0)
        ).height > 0
    
        has_total = final_result.filter(
            pl.col("total_endring").is_not_null()
        ).height > 0
    
        # Assertions with clear messages
        assert has_før, "Pipeline failed: No valid 'naturpoeng_for_inngrep' calculations produced"
        assert has_tapt, "Pipeline failed: No valid 'naturpoeng_tapt' calculations produced"
        assert (has_skapt_onsite or has_skapt_offsite), "Pipeline failed: No valid 'skapt' calculations (neither onsite nor offsite)"
        assert has_total, "Pipeline failed: No valid 'total_endring' calculations produced"
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Tester beregningsfunksjoner""")
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
