PYTHON   := python3
TICKER   := HOOD
QUARTERS := 10

.PHONY: build extract transform model test validate clean clean-all help

# Default target
help:
	@echo "Usage:"
	@echo "  make build      Run full pipeline: extract, transform, model"
	@echo "  make extract    Pull SEC filings and write raw CSVs to data/"
	@echo "  make transform  Clean raw CSVs and write model-ready CSVs to data/"
	@echo "  make model      Build HOOD_Financial_Model.xlsx from model CSVs"
	@echo "  make validate   Run model integrity checks (exit 1 on failure)"
	@echo "  make clean      Remove generated data/output files"

# Full pipeline
build: extract transform model

# Individual steps
extract:
	$(PYTHON) -m src.hood_sec_extract_v3 --ticker $(TICKER) --quarters $(QUARTERS)

transform:
	$(PYTHON) -m src.hood_data_transform

model:
	$(PYTHON) -m src.build_excel_model

test:
	$(PYTHON) -m pytest tests/ -v

validate:
	$(PYTHON) -m src.validate_model

# Cleanup
clean:
	rm -f data/HOOD_*.csv
	rm -f data/model_*.csv
	rm -f data/manifest.json
	rm -f output/HOOD_Financial_Model*.xlsx
	@echo "Cleaned generated files."

clean-all: clean
	rm -rf sec_downloads/
	@echo "Cleaned generated files and SEC download cache."
