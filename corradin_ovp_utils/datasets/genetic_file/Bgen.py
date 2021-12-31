# AUTOGENERATED! DO NOT EDIT! File to edit: datasets_bgen_file_format.ipynb (unless otherwise specified).

__all__ = ['index_search', 'BgenFileObject', 'BgenFileFormat']

# Cell
from . import GeneticFileFormat, GeneticFileFormatInterfaceAPI, get_geno_one_snp, get_possible_geno_combinations
from typing import Any, Dict, List, Optional, Literal, Union, Protocol
from pydantic import BaseModel, PrivateAttr
import pandas as pd
from bgen_reader import open_bgen, read_bgen
from bgen_reader._bgen2 import open_bgen as open_bgen_file_type
import dask.dataframe as dd
from dask.delayed import Delayed
from fastcore.meta import delegates
import numpy as np
import logging
from collections import defaultdict

# Cell
def index_search(list_to_search, query_list):
    sorter = np.argsort(list_to_search)
    index = sorter[np.searchsorted(list_to_search, query_list, sorter=sorter)]
    return index

# class BgenFileObject(BaseModel):
#     variants: dd.DataFrame
#     samples: pd.Series
#     genotype: List[Delayed]
#     bgen_reader_obj: open_bgen

#     class Config:
#         arbitrary_types_allowed = True

#     def __repr__(self):
#         return str(self.__class__) + f" {self.samples.shape[0]} samples"

#     def get_variant_index(self,rsids=None):
#         variant_index = np.argwhere(np.isin(self.bgen_reader_obj.rsids, rsids)).reshape(-1,) if rsids is not None else None
#         return variant_index

#     def get_sample_index(self, sample_ids=None):
#         sample_index = np.argwhere(np.isin(self.samples.values, sample_ids)).reshape(-1,) if sample_ids is not None else None
#         return sample_index

#     def get_probs(self, sample_ids=None, rsids=None):
#         variant_index = self.get_variant_index(rsids)
#         sample_index = self.get_sample_index(sample_ids)
#         return self.bgen_reader_obj.read((sample_index, variant_index))


#     def get_geno_each_sample(self, probs, prob_to_geno_func:Literal["max", "stringent"]= "stringent", high_lim=0.9, low_lim=0.3, NA_val=np.nan):
#         if prob_to_geno_func == "max":
#             geno = np.nanargmax(probs, axis=2).astype(float)

#         elif prob_to_geno_func == "stringent":
#             geno = np.apply_along_axis(get_geno_one_snp, axis=2, arr=probs, high_lim=high_lim, low_lim=low_lim, NA_val=NA_val)

#         return geno


#     def get_allele_ids(self, rsids = None, variant_index = None):
#         if variant_index is None:
#             variant_index = self.get_variant_index(rsids)
#         df = pd.DataFrame([allele_str.split(",") for allele_str in self.bgen_reader_obj.allele_ids[variant_index]], columns = ["allele_1", "allele_2"])

#         if rsids is not None:
#             df.index = rsids
#         return df

#     def get_variant_combinations(self, rsids = None, variant_index = None):
#         if variant_index is None:
#             variant_index = np.argwhere(np.isin(self.bgen_reader_obj.rsids, rsids)).reshape(-1,) if rsids is not None else None
#         geno_df = self.get_allele_ids(rsids, variant_index)
#         geno_df = get_possible_geno_combinations(geno_df, "allele_1", "allele_2")
#         return geno_df

#     #def get_genotypes_df(self, rsid_list: List = None, **kwargs):


class BgenFileObject():
#     variants: dd.DataFrame
#     samples: pd.Series
#     genotype: List[Delayed]

    bgen_reader_obj: open_bgen

    def __init__(self, bgen_reader_obj):
        self.bgen_reader_obj = bgen_reader_obj
        self.samples = np.vstack(np.char.split(self.bgen_reader_obj.samples, sep = " "))[:,0]

    #to do: use fastcore "delegate" function
    @property
    def ids(self):
        return self.bgen_reader_obj.ids

    @property
    def rsids(self):
        return self.bgen_reader_obj.rsids

    def __repr__(self):
        return str(self.__class__) + f" {self.samples.shape[0]} samples"

    def get_variant_index(self,ids=None, id_type = "rsid"):
        if id_type == "id":
            variant_index = index_search(self.ids, ids) if ids is not None else None
        elif id_type == "rsid":
            variant_index = index_search(self.rsids, ids) if ids is not None else None
        return variant_index

    def get_sample_index(self, sample_ids=None):
        sample_index = index_search(self.samples, sample_ids) if sample_ids is not None else None
        return sample_index

    def get_probs(self, samples_index=None, variants_index = None):#get_probs(self, sample_ids=None, variant_ids=None):
        # variant_index = self.get_variant_index(variant_ids)
        # sample_index = self.get_sample_index(sample_ids)

        return self.bgen_reader_obj.read((samples_index, variants_index))


    def get_all_geno_df(self, variant_ids, variants_index):
        all_geno_df = pd.DataFrame({"id_col": variant_ids, "raw_alleles":self.bgen_reader_obj.allele_ids[variants_index]}).set_index("id_col")
        all_geno_df[["alleleA", "alleleB"]] = all_geno_df["raw_alleles"].str.split(",", expand = True)
        all_geno_df = get_possible_geno_combinations(all_geno_df, allele_1_col="alleleA", allele_2_col= "alleleB")
        return all_geno_df

    def get_geno_each_sample(self,
                             sample_ids=None,
                             variant_ids=None,
                             variant_id_type="rsid",
                             prob_to_geno_func:Union["max", "stringent"]= "stringent",
                             high_lim=0.9,
                             low_lim=0.3,
                             NA_val=np.nan,
                             geno_format: Literal["raw", "ohe", "str"]= "raw",
                             **kwargs):
        num_variants_to_search = len(variant_ids)
        variants_index = self.get_variant_index(ids = variant_ids, id_type= variant_id_type)

        if variant_id_type not in ["id", "rsid", "ids", "rsids"]:
            raise ValueError
        if variant_id_type in ["id", "rsid"]:
            variant_id_type += "s"

        #confirming that we have the correct variants by reindexing
        list_of_variants_in_file_specific_id_type = getattr(self.bgen_reader_obj, variant_id_type)
        confirmed_correct_variants_bool_mask = (list_of_variants_in_file_specific_id_type[variants_index]) == np.array(variant_ids)


        confirmed_correct_variants_index = variants_index[confirmed_correct_variants_bool_mask]
        found_variants = np.array(variant_ids)[confirmed_correct_variants_bool_mask]
        #assert all(self.bgen_reader_obj.rsids[variants_index] == variant_ids)
        not_found_variants = np.array(variant_ids)[~confirmed_correct_variants_bool_mask]

        logging.warning(f"\nFound variants: {found_variants.shape[0]}/{num_variants_to_search}\n Not found: {not_found_variants.shape[0]}/{num_variants_to_search}.\n Percent found {round((found_variants.shape[0]/num_variants_to_search) *100)}%")

        samples_index = self.get_sample_index(sample_ids = sample_ids)
        probs = self.get_probs(samples_index=samples_index, variants_index = confirmed_correct_variants_index)

        if prob_to_geno_func == "max":
            geno = np.nanargmax(probs, axis=2).astype(float)

        elif prob_to_geno_func == "stringent":
            geno = np.apply_along_axis(get_geno_one_snp, axis=2, arr=probs, high_lim=high_lim, low_lim=low_lim, NA_val=NA_val)



        geno_no_nan = np.nan_to_num(geno, nan=3).astype(int)
        all_geno_df = self.get_all_geno_df(variant_ids = found_variants, variants_index = confirmed_correct_variants_index)

        if geno_format == "ohe":
            geno_final = np.identity(4)[geno_no_nan]
        elif geno_format == "str":
            geno_final = self.process_raw_geno_data(raw_genos_matrix = geno, all_geno_df = all_geno_df, variant_ids = found_variants)
        else:
            geno_final = geno
        return geno_final, all_geno_df, found_variants, not_found_variants


    def process_raw_geno_data(self, raw_genos_matrix, all_geno_df, variant_ids):

        genos_str = np.select([raw_genos_matrix == 0,
                               raw_genos_matrix ==1,
                               raw_genos_matrix == 2],
                              choicelist=[all_geno_df["AA"], all_geno_df["AB"], all_geno_df["BB"]],
                              default = np.nan)

        all_samples_df = pd.DataFrame(genos_str, columns = variant_ids, index = self.samples)
        all_samples_df.index.name = "sample_id"
        return all_samples_df

    def get_allele_ids(self, rsids = None, variant_index = None):
        if variant_index is None:
            variant_index = self.get_variant_index(rsids)
        df = pd.DataFrame([allele_str.split(",") for allele_str in self.bgen_reader_obj.allele_ids[variant_index]], columns = ["allele_1", "allele_2"])

        if rsids is not None:
            df.index = rsids
        return df

    def get_variant_combinations(self, rsids = None, variant_index = None):
        if variant_index is None:
            variant_index = self.get_variant_index(rsids).reshape(-1,) if rsids is not None else None
        geno_df = self.get_allele_ids(rsids, variant_index)
        geno_df = get_possible_geno_combinations(geno_df, "allele_1", "allele_2")
        return geno_df



class BgenFileFormat(GeneticFileFormat, GeneticFileFormatInterfaceAPI):
    _all_chrom_dict: Dict[int, open_bgen_file_type] = PrivateAttr()
    # = PrivateAttr()

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        self._all_chrom_dict = None #{chrom_number : self.load_one_chrom(chrom_number) for chrom_number in range(1,23)}
        self.sample_ids = None #self._all_chrom_dict[1].samples

    @property
    def _samples_int(self):
        return self.sample_ids.astype(int)

    def get_geno_matrix_specific_chrom(self, *, chrom, chrom_specific_variant_ids, sample_id_subset, variant_id_type):
        logging.warning(f"Loading {len(chrom_specific_variant_ids)} SNPs with id type '{variant_id_type}' for chromosome {chrom}, {len(sample_id_subset)} people")
        all_samples_df_specific_chrom, all_geno_df_specific_chrom, found_variants, not_found_variants = self._all_chrom_dict[chrom].get_geno_each_sample(prob_to_geno_func = "stringent",
                                                                                         sample_ids=sample_id_subset,
                                                                                         variant_ids=chrom_specific_variant_ids,
                                                                                         variant_id_type= variant_id_type,
                                                                                         geno_format = "str")
        return  all_samples_df_specific_chrom, all_geno_df_specific_chrom, found_variants, not_found_variants

    #TODO: accomodate different types of SNP id
    def get_geno_each_sample(self, rsid_dict:Dict[int, List[str]], id_col_list: List[str] = [], batch_size=1_000, excluded_sample_ids: List[str] =[]):
        if self._all_chrom_dict is None or self.sample_ids is None:
            chromosomes_needed = list(rsid_dict.keys())
            logging.warning(f"Initialize dictionary of genetic files for {len(chromosomes_needed)} chromosomes, this might take a while")
            self._all_chrom_dict = {chrom_number : self.load_one_chrom(chrom_number) for chrom_number in rsid_dict.keys()}
            self.sample_ids = self._all_chrom_dict[chromosomes_needed[0]].samples
        all_samples_df_list = []
        all_geno_df_list = []
        found_variants_dict= defaultdict(list)
        not_found_variants_dict=defaultdict(list)

        sample_id_subset = list(set(self.sample_ids) - set(excluded_sample_ids))

        #print(sample_id_subset)
        for chrom, chrom_specific_variant_ids in rsid_dict.items():
            rsids_list = list(set([variant_id for variant_id in chrom_specific_variant_ids if variant_id.startswith("rs")]))
            if rsids_list:
                all_samples_df_specific_chrom, all_geno_df_specific_chrom, found_variants_specific_chrom, not_found_variants_specific_chrom = self.get_geno_matrix_specific_chrom(chrom = chrom,
                                                                                                                chrom_specific_variant_ids= rsids_list,
                                                                                                                sample_id_subset= sample_id_subset,
                                                                                                                variant_id_type = "rsid")

                all_samples_df_list.append(all_samples_df_specific_chrom)
                all_geno_df_list.append(all_geno_df_specific_chrom)
                found_variants_dict[chrom].append(found_variants_specific_chrom)
                not_found_variants_dict[chrom].append(not_found_variants_specific_chrom)

            ids_list = [id_str for id_str in (set(chrom_specific_variant_ids) - set(rsids_list)) if self.check_if_id(id_str)]
            if ids_list:
                all_samples_df_specific_chrom, all_geno_df_specific_chrom = self.get_geno_matrix_specific_chrom(chrom = chrom,
                                                                                                                chrom_specific_variant_ids= ids_list,
                                                                                                                sample_id_subset= sample_id_subset,
                                                                                                                variant_id_type = "id")

                all_samples_df_list.append(all_samples_df_specific_chrom)
                all_geno_df_list.append(all_geno_df_specific_chrom)
                found_variants_dict[chrom].append(found_variants_specific_chrom)
                not_found_variants_dict[chrom].append(not_found_variants_specific_chrom)

        not_found_variants_sum_dict = {chrom: len(variant_list) for chrom, variant_list in not_found_variants_dict.items()}
        found_variants_sum_dict = {chrom: len(variant_list) for chrom, variant_list in found_variants_dict.items()}

        total_variants_found = sum([sum_each_chrom for sum_each_chrom in found_variants_sum_dict.values()])
        total_variants_not_found = sum([sum_each_chrom for sum_each_chrom in not_found_variants_sum_dict.values()])
        logging.warning(f"Number of found variants per chromosome: {found_variants_sum_dict}")
        logging.warning(f"Number of not found variants per chromosome: {not_found_variants_sum_dict}")

        return pd.concat(all_samples_df_list), pd.concat(all_geno_df_list)


    def check_if_id(self, variant_string):
        chrom, rest = variant_string.split(":")
        if 1 <= chrom <= 22 and len(rest.split("_")) == 3:
            return True
        else:
            return False

    @delegates(read_bgen)
    def load_one_chrom(self, chrom=None, **kwargs):
        file_path = self.get_resolved_file_path(chrom=chrom)
        print(f"Loading chromosome {chrom}")
        open_bgen_obj = open_bgen(file_path, samples_filepath= self.sample_file, allow_complex = False)
        return BgenFileObject(bgen_reader_obj = open_bgen_obj)
