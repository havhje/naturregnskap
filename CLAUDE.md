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

1. **Data Processing Pipeline**:
   - Loads reference data from Excel files
   - Uses DuckDB for SQL-based data querying
   - Processes data with Polars DataFrames

2. **Calculation Engine**:
   - Implements the "Naturpoeng" (Nature Points) calculation system
   - Key formulas:
     - `Naturpoeng før inngrep = Areal (KNO-polygoner) × Naturkvalitet × Forvaltningsinteresse`
     - `Naturpoeng tapt = Endring (reduksjon) i total naturverdi`
     - `Naturpoeng skapt = (Areal × Naturkvalitet × Forvaltningsinteresse) × R(avstand) × R(tid) × R(vanskelighetsgrad)`

3. **Reference Data Tables**:
   - `avstand fra inngrep.xlsx` - Distance risk factors
   - `forvaltningsinteresse.xlsx` - Management interest multipliers
   - `naturkvalitet.xlsx` - Nature quality scores
   - `påvirkning.xlsx` - Impact assessment factors
   - `tidsperspektiv.xlsx` - Time perspective risk factors
   - `vanskelighetsgrad.xlsx` - Difficulty grade risk factors

### Marimo-Specific Patterns
- Uses `mo.ui` components for interactive elements
- Employs `hide_code=True` to create clean user interfaces
- Leverages Marimo's reactive cell execution model
- Utilizes `mo.vstack` and column layouts for UI organization

### Data Flow
1. Excel files → DuckDB tables
2. User inputs via Marimo UI components
3. SQL queries process and join data
4. Polars DataFrames for final calculations
5. Results displayed in formatted tables

## Key Technical Considerations

1. **Marimo Reactivity**: Changes to UI components automatically trigger dependent cell re-execution
2. **DuckDB Integration**: In-memory database for efficient SQL operations on Excel data
3. **Formula Implementation**: Complex environmental accounting formulas are embedded in the notebook cells
4. **UI State Management**: Marimo handles state through reactive variables

## Common Development Tasks

When modifying calculations:
1. Locate the relevant formula cell in `Naturregnskap.py`
2. Update the SQL query or calculation logic
3. Test with the synthetic example data provided
4. Verify all dependent cells update correctly

When adding new reference data:
1. Add the Excel file to the project root
2. Create a new cell to load it into DuckDB
3. Update relevant SQL queries to join the new data
4. Adjust formulas as needed