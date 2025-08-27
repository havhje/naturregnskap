# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Norwegian "Naturregnskap" (Nature Accounting) application built using Marimo, an interactive Python notebook framework. The application calculates environmental impact scores using a point-based system to evaluate biodiversity and nature conservation efforts.

## Development Commands

### Running the Application
```bash
# Run the main Marimo notebook
marimo run Naturregnskap.py

# Edit the notebook interactively
marimo edit Naturregnskap.py
```

### Package Management
This project uses UV as the package manager:
```bash
# Install dependencies
uv pip sync pyproject.toml

# Add a new dependency
uv pip install <package-name>

# Update lock file
uv lock
```

### Python Environment
- Python version: 3.11 (specified in `.python-version`)
- Virtual environment: `.venv` directory

## Architecture Overview

### Core Application Structure
The application is built as a single Marimo notebook (`Naturregnskap.py`) that implements:

1. **Complete Data Processing Pipeline**:
   - Loads all 6 reference CSV files (converted from Excel)
   - Uses Polars DataFrames for data processing and calculations
   - Implements comprehensive data cleaning and text normalization for Norwegian CSV format
   - Supports complex multi-table joins with risk factors and impact assessments

2. **Complete Calculation Engine**:
   - Implements the full "Naturpoeng" (Nature Points) calculation system
   - Supports all 4 calculation types plus summary totals
   - Key formulas implemented:
     - `Naturpoeng før inngrep = (Utstrekning før inngrep) × (Naturkvalitet før inngrep) × (Forvaltningsinteresse før inngrep)`
     - `Naturpoeng tapt = (Tapt utstrekning × Naturkvalitet × Forvaltningsinteresse) + (Resterende utstrekning × Naturkvalitet × Forvaltningsinteresse × Påvirkningsfaktor)`
     - `Naturpoeng skapt onsite = (Utstrekning etter tiltak × Mål naturkvalitet × Forvaltningsinteresse) × R(avstand) × R(tid) × R(vanskelighetsgrad)`
     - `Naturpoeng skapt offsite = {(Utstrekning etter tiltak × Mål naturkvalitet × Forvaltningsinteresse) - (Utstrekning før tiltak × Naturkvalitet × Forvaltningsinteresse)} × R(risikofaktorer)`
     - `Total endring = Σ(Naturpoeng før) - Σ(Naturpoeng tapt) + Σ(Naturpoeng skapt)`

3. **Complete Reference Data Tables** (CSV format):
   - `naturkvalitet.csv` - Nature quality scores and definitions
   - `forvaltningsinteresse.csv` - Management interest multipliers
   - `avstand fra inngrep.csv` - Distance risk factors (0.5-1.0)
   - `påvirkning.csv` - Impact factors (0.25-1.0)
   - `tidsperspektiv.csv` - Time risk factors (0.3-0.9)
   - `vanskelighetsgrad.csv` - Difficulty risk factors (0.1-1.0)

4. **Enhanced Synthetic Test Data**:
   - Three test areas (delområde A, B, C) with 16 comprehensive data fields
   - Covers Verneområder, Funksjonsområder, and Vannforekomster
   - Includes all risk factors, impact assessments, and before/after scenarios

### Marimo-Specific Patterns
- Uses `mo.ui` components for interactive elements
- Employs `hide_code=True` to create clean user interfaces
- Leverages Marimo's reactive cell execution model
- Cell dependencies managed through explicit return statements
- Function definitions returned from cells for reuse across the notebook

### Data Flow
1. **Enhanced Data Loading**: All 6 CSV files → Polars DataFrames with Norwegian format handling (semicolon separator, decimal comma)
2. **Comprehensive Data Cleaning**: Text columns normalized (whitespace trimming, type casting) for all lookup tables
3. **Complex Multi-table Joins**: Joins on (`arealtype`, `regnskapstema`, quality/interest levels) plus risk factor lookups with aliased columns for før/mål distinctions
4. **Complete Calculation Pipeline**: Sequential function application:
   - `calculate_naturpoeng_for_inngrep()` - Base calculation
   - `calculate_naturpoeng_tapt()` - Loss calculation with dual components
   - `calculate_naturpoeng_skapt_onsite()` - Onsite creation with risk factors
   - `calculate_naturpoeng_skapt_offsite()` - Offsite net improvement calculation
   - `calculate_total_endring()` - Summary totals and final impact assessment
5. **Comprehensive Results**: Complete DataFrame with all calculations plus formatted summary display

## Key Technical Considerations

1. **Marimo Reactivity**: Changes to UI components automatically trigger dependent cell re-execution
2. **Norwegian CSV Format**: Semicolon separators, decimal commas, UTF-8 encoding
3. **Data Type Handling**: Explicit casting required for numeric operations (use `.cast(pl.Float64)`)
4. **Text Normalization**: Join keys require `.str.strip_chars()` to handle whitespace issues
5. **Function Architecture**: All calculation functions are modular and reusable across cells
6. **Complex Multi-Key Joins**: Advanced joins on 3+ columns for precise lookup matching with aliased columns
7. **Risk Factor Integration**: All risk factors properly integrated with appropriate scaling (0.1-1.0 ranges)
8. **Dual-Component Calculations**: Tapt calculation handles both direct loss and impact-scaled remaining areas
9. **Sequential Calculation Pipeline**: Functions must be applied in correct order due to column dependencies

## Common Development Tasks

### When modifying calculations:
1. Locate the relevant calculation function in `Naturregnskap.py`
2. Update the function logic while maintaining the input/output contract
3. Test with the synthetic example data provided (delområde A, B, C)
4. Verify all dependent cells update correctly due to Marimo's reactivity

### When adding new reference data:
1. Add the CSV file to the project root (Norwegian format: `;` separator, decimal comma)
2. Create a new cell to load it with proper data cleaning:
   ```python
   df = pl.read_csv("file.csv", separator=";", encoding="utf-8", decimal_comma=True)
   .with_columns([pl.col("text_column").str.strip_chars()])
   ```
3. Update join operations to include the new lookup data
4. Modify calculation functions as needed

### Data Cleaning Best Practices:
- Always apply `.str.strip_chars()` to text columns used in joins
- Use explicit schema overrides for numeric columns: `schema_overrides={'numeric_col': pl.Float64}`
- Cast columns in calculation functions: `.cast(pl.Float64)` before arithmetic operations
- Handle Norwegian decimal format with `decimal_comma=True`

### Current Implementation Status:
- ✅ "Naturpoeng (før inngrep)" calculation - **COMPLETE**
- ✅ "Naturpoeng (tapt)" calculation with dual-component formula - **COMPLETE**
- ✅ "Naturpoeng (skapt onsite)" calculation with risk factors - **COMPLETE**  
- ✅ "Naturpoeng (skapt offsite)" calculation with net improvement - **COMPLETE**
- ✅ "Total endring" summary calculations - **COMPLETE**
- ✅ All risk factor integrations (avstand, tid, vanskelighetsgrad, påvirkning) - **COMPLETE**
- ✅ Enhanced synthetic dataset with 16 comprehensive fields - **COMPLETE**
- ✅ Complete multi-table join pipeline - **COMPLETE**
- ✅ Formatted results display with summary totals - **COMPLETE**

### Application Features:
- **Complete Naturregnskap System**: All calculations implemented according to Norwegian environmental standards
- **Risk-Adjusted Calculations**: Full integration of distance, time, difficulty, and impact factors
- **Comprehensive Test Data**: Representative scenarios covering different ecosystem types
- **Production Ready**: Clean Marimo interface with organized cell structure and proper data validation