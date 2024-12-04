import os
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from pathlib import Path


class SampleSheetValidator:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path)
        self.schema = DataFrameSchema(
            {
                "sample_id": Column(
                    str,
                    nullable=False,
                    required=True,
                    coerce=True,
                    checks=Check.str_matches(r"^[a-zA-Z0-9_]+$"),
                ),
                "sra_accession": Column(str, nullable=True, required=False),
                "read_1": Column(str, nullable=True, required=False),
                "read_2": Column(str, nullable=True, required=False),
                "bam": Column(str, nullable=True, required=False),
                "library_id": Column(str, nullable=True, required=False),
            },
            checks=pa.Check(
                lambda df: df.apply(self.validate_dependencies, axis=1),
                element_wise=False,
            ),
        )
        # Instance variables to track the presence of headers
        self.bam_present = "bam" in self.df.columns
        self.sra_present = "sra_accession" in self.df.columns
        self.lib_id_present = "library_id" in self.df.columns
        self.reads_present = "read_1" in self.df.columns and "read_2" in self.df.columns

    def validate_header(self) -> None:
        """
        Validates the presence and mutual exclusivity of required headers.
        """
        if not any([self.bam_present, self.sra_present, self.reads_present]):
            raise ValueError(
                "Invalid sample sheet header: Must supply 'bam', 'sra_accession', or 'read_1'/'read_2' in sample sheet."
            )

        if self.bam_present:
            if self.sra_present or self.reads_present:
                raise ValueError(
                    "Invalid sample sheet header: cannot supply 'bam' with either 'sra_accession' or 'read_1'/'read_2'."
                )

        if self.reads_present and not self.lib_id_present:
            raise ValueError(
                "Invalid sample sheet header: must supply 'library_id' when using 'read_1'/'read_2'."
            )

    def validate_dependencies(self, row: pd.Series) -> bool:
        """
        Validates mutual exclusivity and dependencies at the row level.
        """
        errors = []

        # Check presence of each group
        if self.sra_present and self.reads_present:
            sra_present = pd.notna(row["sra_accession"])
            reads_present = pd.notna(row["read_1"]) and pd.notna(row["read_2"])

            # Rule: sra_accession and (read_1, read_2) cannot coexist in the same row
            if sra_present and reads_present:
                errors.append(
                    "Both 'sra_accession' and 'read_1'/'read_2' are provided in the same row."
                )

        # Return True if no errors; otherwise raise ValueError with detailed messages
        if not errors:
            return True
        raise ValueError(f"Row failed validation: {', '.join(errors)}")

    def validate_and_assign_library_id(self) -> pd.DataFrame:
        """
        Validates and assigns library_id for sample_ids in the sample sheet.
        - If a sample_id group has >1 row, all rows must have a defined library_id.
        - If a sample_id group has exactly 1 row and library_id is NaN, it is set to the sample_id.
        """
        errors = []

        # Check for duplicates in sample_id + sra_accession combos
        if self.sra_present:
            duplicate_sra_accession = self.df[
                self.df.duplicated(subset=["sample_id", "sra_accession"], keep=False)
            ]
            if not duplicate_sra_accession.empty:
                errors.append(
                    f"Duplicate sample_id and sra_accession combinations detected:\n{duplicate_sra_accession[['sample_id', 'sra_accession']]}"
                )

        # Check for duplicates in sample_id + read_1/read_2 combinations
        if self.reads_present:
            duplicate_reads = self.df[
                self.df.duplicated(subset=["sample_id", "read_1", "read_2"], keep=False)
            ]
            if not duplicate_reads.empty:
                errors.append(
                    f"Duplicate sample_id and read paths detected:\n{duplicate_reads[['sample_id', 'read_1', 'read_2']]}"
                )

        if self.reads_present and self.lib_id_present:
            sample_library_groups = self.df.groupby("sample_id")
            for sample_id, group in sample_library_groups:
                if len(group) > 1:
                    # For groups with >1 row, all rows must have a library_id
                    if group["library_id"].isna().all():
                        errors.append(
                            f"Sample_id '{sample_id}' has multiple rows but no library_id defined."
                        )
                else:
                    # For groups with exactly 1 row, set library_id to sample_id if NaN
                    if pd.isna(group["library_id"].iloc[0]):
                        self.df.loc[group.index, "library_id"] = sample_id

        # If any errors were detected, raise an exception
        if errors:
            raise ValueError("\n".join(errors))

        return self.df

    def update_df_paths(self, outdir: str) -> None:
        """
        Updates the paths in read_1, read_2, and bam columns to be relative to the specified outdir.
        """
        for col in ["read_1", "read_2", "bam"]:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(
                    lambda x: os.path.relpath(x, outdir) if pd.notna(x) else x
                )

    def validate_samplesheet(self, outdir=None) -> pd.DataFrame:
        """
        Validates the sample sheet by performing header validation, row-level validation, and library ID assignment.
        Optionally updates paths to be relative to the specified outdir.
        """
        self.validate_header()
        validated_df = self.schema.validate(self.df)
        self.df = self.validate_and_assign_library_id()
        if outdir is not None:
            self.update_df_paths(outdir)
        return self.df
