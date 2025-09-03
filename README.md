# magmerge

Parsers and merging of outputs (DAS Tool, samtools coverage, GTDB-Tk) + CLI

## Installation

### 1. Clone the repository
```bash
git clone https://github.com/ # TO DO !!!
cd magmerge
```

### 2. Install dependencies with [Poetry](https://python-poetry.org/docs/#installation)
If you already have Poetry installed:
```bash
poetry install
```

This will:
- create a virtual environment,
- install all required dependencies (main + dev).

Activate the environment with:
```bash
poetry shell
```

---

### 3. If you don’t have Poetry installed

You can either install Poetry or use `pip` directly.

**Option A – Install Poetry (recommended):**
```bash
pip install poetry
poetry install
```

**Option B – Use pip instead of Poetry:**
```bash
python -m venv .venv
source .venv/bin/activate   # Linux 

pip install -e .[dev]
```

The `-e .` flag installs the project in editable mode.  
The `[dev]` extra installs development dependencies (pytest, black, etc.).

---

### 4. Run tests
You can run pytest & black & test bash script inside a notebook (`nb_dev.ipynb`)

**Python (magmerge):**
- Parses outputs from **DAS Tool**, **samtools coverage**, and **GTDB-Tk**.
- Skips incomplete samples (with a clear log message).
- Produces a per-MAG table with: `mag_id`, `genome_size`, `bin_score`, `relative_abundance`,
  taxonomy (Domain→Species), `closest_reference_genome_id`, `closest_reference_genome_ani`.

**Bash (fetch_sra.sh):**
- Reads SRA IDs from a text file.
- Downloads reads (optionally partial), gzips and organizes them as:
  `output_dir/<sample>/<sample>_1.fastq.gz` and `_2.fastq.gz`.
- Prints a mini-report of successes/failures.

## Quick usage

You can run the pipelines directly inside a notebook (`nb.ipynb`).