# Outside Variants utilities functions
> Library of functions for the Outside Variant Pipeline to create unifying API and facilitate interactive exploration, so anyone can run the Outside Variant Pipeline


[![CI](https://github.com/corradin-lab/corradin-ovp-utils/actions/workflows/main.yml/badge.svg)](https://github.com/corradin-lab/corradin-ovp-utils/actions/workflows/main.yml)

## Install

`pip install corradin_ovp_utils`

## How to use

This library is created to create a unifying API to read in different genetic data formats and specification, combine with phenotype data to create the core data structure of the outside variant pipeline. The library decouples data ingestion from downstream analyses, and make it extremely easy to extend to other input data formats while maintaining the core analysis features

### Quick Start

After specifying the input in a `yaml` file (see `conf/base/catalog_input/genetic_file.yaml` and `conf/base/catalog_input/sample_file.yaml`), we can load the datasets in like this:

```python
#collapse_input closed

from kedro.config import ConfigLoader
from kedro.io import DataCatalog
conf_loader = ConfigLoader("conf/base")
conf_test_data_catalog = conf_loader.get("catalog*.yaml", "catalog*/*.yaml")
test_data_catalog = DataCatalog.from_config(conf_test_data_catalog)
```

Each of them is an `OVPDataset` with a unifying API

```python
genetic_file = test_data_catalog.load("genetic_file")
sample_file = test_data_catalog.load("sample_file")

genetic_file, sample_file
```




    (<corradin_ovp_utils.datasets.OVPDataset.OVPDataset at 0x7fd8383aa4f0>,
     <corradin_ovp_utils.datasets.OVPDataset.OVPDataset at 0x7fd878c9fcd0>)



We can see that the `genetic_file` and `sample_file` contains two files, case and control

```python
genetic_file.full_file_path
```




    {'case': PosixPath('data/test_data/gen_file/test_CASE_MS_chr22.gen'),
     'control': PosixPath('data/test_data/gen_file/test_CONTROL_MS_chr22.gen')}



```python
sample_file.full_file_path
```




    {'case': PosixPath('data/test_data/sample_file/MS_impute2_ALL_sample_out.tsv'),
     'control': PosixPath('data/test_data/sample_file/ALL_controls_58C_NBS_WTC2_impute2_sample_out.tsv')}



To extract the core data structure of the outside variant pipeline, you need:
- Genetic file
- Sample file
- List of Rsid to extract information

Just feed these inputs into the `CombinedGenoPheno` object to get back a dataframe of genotypes for those SNPs for all of the samples (both case and control). **This is the core data structure of the pipeline**.

```python
all_samples_geno_df = CombinedGenoPheno.init_from_OVPDataset(genetic_file, sample_file, rsid_list = ["rs77948203", "rs9610458", "rs134490", "rs5756405"])
all_samples_geno_df
```

    /Users/ahoang/Documents/Learning/corradin_ovp_utils/.venv/lib/python3.8/site-packages/ipykernel/ipkernel.py:283: DeprecationWarning: `should_run_async` will not call `transform_cell` automatically in the future. Please pass the result to `transformed_cell` argument and any exception that happen during thetransform in `preprocessing_exc_tuple` in IPython 7.17 and above.
      and should_run_async(code)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th>rsid</th>
      <th>rs77948203</th>
      <th>rs9610458</th>
      <th>rs134490</th>
      <th>rs5756405</th>
    </tr>
    <tr>
      <th>sample_id</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>WTCCCT473540</th>
      <td>GG</td>
      <td>TT</td>
      <td>NA</td>
      <td>AG</td>
    </tr>
    <tr>
      <th>WTCCCT473530</th>
      <td>GG</td>
      <td>TT</td>
      <td>TT</td>
      <td>AA</td>
    </tr>
    <tr>
      <th>WTCCCT473555</th>
      <td>GG</td>
      <td>TT</td>
      <td>TT</td>
      <td>NA</td>
    </tr>
    <tr>
      <th>WTCCCT473426</th>
      <td>GG</td>
      <td>TT</td>
      <td>TT</td>
      <td>GG</td>
    </tr>
    <tr>
      <th>WTCCCT473489</th>
      <td>GG</td>
      <td>CT</td>
      <td>NA</td>
      <td>AA</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>WS574632</th>
      <td>GG</td>
      <td>CT</td>
      <td>TT</td>
      <td>GG</td>
    </tr>
    <tr>
      <th>WS574661</th>
      <td>GG</td>
      <td>TT</td>
      <td>TT</td>
      <td>AA</td>
    </tr>
    <tr>
      <th>BLOOD294452</th>
      <td>GG</td>
      <td>CT</td>
      <td>TT</td>
      <td>AG</td>
    </tr>
    <tr>
      <th>WTCCCT511021</th>
      <td>GG</td>
      <td>CT</td>
      <td>TT</td>
      <td>AG</td>
    </tr>
    <tr>
      <th>WTCCCT510948</th>
      <td>GG</td>
      <td>CT</td>
      <td>TT</td>
      <td>AA</td>
    </tr>
  </tbody>
</table>
<p>14947 rows × 4 columns</p>
</div>



We can then take this output dataframe and do downstream analysis with it, using the functions in this library. For example, let's see the break down of genotypes grouped by the two SNPs `"rs77948203"` and `"rs9610458"`

```python
get_geno_combination_df(geno_each_sample_df=all_samples_geno_df, 
                       rsid_list= ["rs77948203", "rs9610458"], as_df = True)
```

    /Users/ahoang/Documents/Learning/corradin_ovp_utils/.venv/lib/python3.8/site-packages/ipykernel/ipkernel.py:283: DeprecationWarning: `should_run_async` will not call `transform_cell` automatically in the future. Please pass the result to `transformed_cell` argument and any exception that happen during thetransform in `preprocessing_exc_tuple` in IPython 7.17 and above.
      and should_run_async(code)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>rs77948203</th>
      <th>rs9610458</th>
      <th>unique_samples_id</th>
      <th>unique_samples_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AA</td>
      <td>CC</td>
      <td>[WTCCCT470057, WTCCCT489315, WTCCCT508408, WTC...</td>
      <td>19</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AA</td>
      <td>CT</td>
      <td>[WTCCCT474394, WTCCCT470264, WTCCCT470548, WTC...</td>
      <td>34</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AA</td>
      <td>NA</td>
      <td>[WTCCCT474448, WTCCCT508352]</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AA</td>
      <td>TT</td>
      <td>[WTCCCT474560, WTCCCT469955, WTCCCT470219, WTC...</td>
      <td>23</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AG</td>
      <td>CC</td>
      <td>[WTCCCT466268, WTCCCT489637, WTCCCT488814, WTC...</td>
      <td>360</td>
    </tr>
    <tr>
      <th>5</th>
      <td>AG</td>
      <td>CT</td>
      <td>[WTCCCT473524, WTCCCT473551, WTCCCT489609, WTC...</td>
      <td>949</td>
    </tr>
    <tr>
      <th>6</th>
      <td>AG</td>
      <td>NA</td>
      <td>[WTCCCT489613, WTCCCT497565, WTCCCT468278, WTC...</td>
      <td>61</td>
    </tr>
    <tr>
      <th>7</th>
      <td>AG</td>
      <td>TT</td>
      <td>[WTCCCT473522, WTCCCT473497, WTCCCT473514, WTC...</td>
      <td>575</td>
    </tr>
    <tr>
      <th>8</th>
      <td>GG</td>
      <td>CC</td>
      <td>[WTCCCT473500, WTCCCT473552, WTCCCT473505, WTC...</td>
      <td>2593</td>
    </tr>
    <tr>
      <th>9</th>
      <td>GG</td>
      <td>CT</td>
      <td>[WTCCCT473489, WTCCCT473456, WTCCCT473515, WTC...</td>
      <td>6126</td>
    </tr>
    <tr>
      <th>10</th>
      <td>GG</td>
      <td>NA</td>
      <td>[WTCCCT473549, WTCCCT489615, WTCCCT489614, WTC...</td>
      <td>423</td>
    </tr>
    <tr>
      <th>11</th>
      <td>GG</td>
      <td>TT</td>
      <td>[WTCCCT473540, WTCCCT473530, WTCCCT473555, WTC...</td>
      <td>3782</td>
    </tr>
  </tbody>
</table>
</div>



Let's add the third SNP, `"rs134490"`, and see how the break down changes

```python
#collapse_output closed
triple_SNPs = get_geno_combination_df(geno_each_sample_df=all_samples_geno_df, 
                       rsid_list= ["rs77948203", "rs9610458", "rs134490"])
triple_SNPs.df
```

    /Users/ahoang/Documents/Learning/corradin_ovp_utils/.venv/lib/python3.8/site-packages/ipykernel/ipkernel.py:283: DeprecationWarning: `should_run_async` will not call `transform_cell` automatically in the future. Please pass the result to `transformed_cell` argument and any exception that happen during thetransform in `preprocessing_exc_tuple` in IPython 7.17 and above.
      and should_run_async(code)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>rs77948203</th>
      <th>rs9610458</th>
      <th>rs134490</th>
      <th>unique_samples_id</th>
      <th>unique_samples_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AA</td>
      <td>CC</td>
      <td>CT</td>
      <td>[WTCCCT508925, CCC2_MS656176, WTCCCT444162, WT...</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AA</td>
      <td>CC</td>
      <td>NA</td>
      <td>[WTCCCT490220, BLOOD293205]</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AA</td>
      <td>CC</td>
      <td>TT</td>
      <td>[WTCCCT470057, WTCCCT489315, WTCCCT508408, WTC...</td>
      <td>13</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AA</td>
      <td>CT</td>
      <td>CT</td>
      <td>[WTCCCT466178, WTCCCT468665, WTCCCT471002, WTC...</td>
      <td>8</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AA</td>
      <td>CT</td>
      <td>NA</td>
      <td>[WTCCCT474394, WTCCCT470548, WTCCCT443601]</td>
      <td>3</td>
    </tr>
    <tr>
      <th>5</th>
      <td>AA</td>
      <td>CT</td>
      <td>TT</td>
      <td>[WTCCCT470264, WTCCCT449002, WTCCCT467316, WTC...</td>
      <td>23</td>
    </tr>
    <tr>
      <th>6</th>
      <td>AA</td>
      <td>NA</td>
      <td>CT</td>
      <td>[WTCCCT474448]</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>AA</td>
      <td>NA</td>
      <td>NA</td>
      <td>[WTCCCT508352]</td>
      <td>1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>AA</td>
      <td>TT</td>
      <td>CC</td>
      <td>[BLOOD293241]</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>AA</td>
      <td>TT</td>
      <td>CT</td>
      <td>[WTCCCT470219, WTCCCT466993, WTCCCT508309, WTC...</td>
      <td>6</td>
    </tr>
    <tr>
      <th>10</th>
      <td>AA</td>
      <td>TT</td>
      <td>NA</td>
      <td>[WTCCCT442733, WTCCCT543105, WTCCCT543369]</td>
      <td>3</td>
    </tr>
    <tr>
      <th>11</th>
      <td>AA</td>
      <td>TT</td>
      <td>TT</td>
      <td>[WTCCCT474560, WTCCCT469955, WTCCCT471095, WTC...</td>
      <td>13</td>
    </tr>
    <tr>
      <th>12</th>
      <td>AG</td>
      <td>CC</td>
      <td>CC</td>
      <td>[WTCCCT470291, WTCCCT506356, WTCCCT515627, WTC...</td>
      <td>8</td>
    </tr>
    <tr>
      <th>13</th>
      <td>AG</td>
      <td>CC</td>
      <td>CT</td>
      <td>[WTCCCT489637, WTCCCT507947, WTCCCT474442, WTC...</td>
      <td>85</td>
    </tr>
    <tr>
      <th>14</th>
      <td>AG</td>
      <td>CC</td>
      <td>NA</td>
      <td>[WTCCCT488814, WTCCCT497627, WTCCCT508819, WTC...</td>
      <td>51</td>
    </tr>
    <tr>
      <th>15</th>
      <td>AG</td>
      <td>CC</td>
      <td>TT</td>
      <td>[WTCCCT466268, WTCCCT467701, WTCCCT488998, WTC...</td>
      <td>216</td>
    </tr>
    <tr>
      <th>16</th>
      <td>AG</td>
      <td>CT</td>
      <td>CC</td>
      <td>[WTCCCT497897, WTCCCT467963, WTCCCT506246, WTC...</td>
      <td>26</td>
    </tr>
    <tr>
      <th>17</th>
      <td>AG</td>
      <td>CT</td>
      <td>CT</td>
      <td>[WTCCCT489588, WTCCCT467776, WTCCCT467779, WTC...</td>
      <td>242</td>
    </tr>
    <tr>
      <th>18</th>
      <td>AG</td>
      <td>CT</td>
      <td>NA</td>
      <td>[WTCCCT473524, WTCCCT467504, WTCCCT474443, WTC...</td>
      <td>111</td>
    </tr>
    <tr>
      <th>19</th>
      <td>AG</td>
      <td>CT</td>
      <td>TT</td>
      <td>[WTCCCT473551, WTCCCT489609, WTCCCT489577, WTC...</td>
      <td>570</td>
    </tr>
    <tr>
      <th>20</th>
      <td>AG</td>
      <td>NA</td>
      <td>CC</td>
      <td>[WTCCCT443738]</td>
      <td>1</td>
    </tr>
    <tr>
      <th>21</th>
      <td>AG</td>
      <td>NA</td>
      <td>CT</td>
      <td>[WTCCCT468278, WTCCCT474556, WTCCCT515401, WTC...</td>
      <td>22</td>
    </tr>
    <tr>
      <th>22</th>
      <td>AG</td>
      <td>NA</td>
      <td>NA</td>
      <td>[WTCCCT489613, WTCCCT497565, WTCCCT508768, WTC...</td>
      <td>7</td>
    </tr>
    <tr>
      <th>23</th>
      <td>AG</td>
      <td>NA</td>
      <td>TT</td>
      <td>[WTCCCT470293, WTCCCT470261, WTCCCT508484, WTC...</td>
      <td>31</td>
    </tr>
    <tr>
      <th>24</th>
      <td>AG</td>
      <td>TT</td>
      <td>CC</td>
      <td>[WTCCCT507975, WTCCCT467005, WTCCCT466412, WTC...</td>
      <td>10</td>
    </tr>
    <tr>
      <th>25</th>
      <td>AG</td>
      <td>TT</td>
      <td>CT</td>
      <td>[WTCCCT489586, WTCCCT507962, WTCCCT473262, WTC...</td>
      <td>144</td>
    </tr>
    <tr>
      <th>26</th>
      <td>AG</td>
      <td>TT</td>
      <td>NA</td>
      <td>[WTCCCT473497, WTCCCT467773, WTCCCT473218, WTC...</td>
      <td>79</td>
    </tr>
    <tr>
      <th>27</th>
      <td>AG</td>
      <td>TT</td>
      <td>TT</td>
      <td>[WTCCCT473522, WTCCCT473514, WTCCCT489575, WTC...</td>
      <td>342</td>
    </tr>
    <tr>
      <th>28</th>
      <td>GG</td>
      <td>CC</td>
      <td>CC</td>
      <td>[WTCCCT489620, WTCCCT489645, WTCCCT473287, WTC...</td>
      <td>68</td>
    </tr>
    <tr>
      <th>29</th>
      <td>GG</td>
      <td>CC</td>
      <td>CT</td>
      <td>[WTCCCT473552, WTCCCT473505, WTCCCT489578, WTC...</td>
      <td>635</td>
    </tr>
    <tr>
      <th>30</th>
      <td>GG</td>
      <td>CC</td>
      <td>NA</td>
      <td>[WTCCCT489646, WTCCCT489580, WTCCCT488843, WTC...</td>
      <td>328</td>
    </tr>
    <tr>
      <th>31</th>
      <td>GG</td>
      <td>CC</td>
      <td>TT</td>
      <td>[WTCCCT473500, WTCCCT473539, WTCCCT473521, WTC...</td>
      <td>1562</td>
    </tr>
    <tr>
      <th>32</th>
      <td>GG</td>
      <td>CT</td>
      <td>CC</td>
      <td>[WTCCCT473297, WTCCCT473230, WTCCCT473244, WTC...</td>
      <td>165</td>
    </tr>
    <tr>
      <th>33</th>
      <td>GG</td>
      <td>CT</td>
      <td>CT</td>
      <td>[WTCCCT473447, WTCCCT473466, WTCCCT473492, WTC...</td>
      <td>1483</td>
    </tr>
    <tr>
      <th>34</th>
      <td>GG</td>
      <td>CT</td>
      <td>NA</td>
      <td>[WTCCCT473489, WTCCCT473499, WTCCCT473529, WTC...</td>
      <td>845</td>
    </tr>
    <tr>
      <th>35</th>
      <td>GG</td>
      <td>CT</td>
      <td>TT</td>
      <td>[WTCCCT473456, WTCCCT473515, WTCCCT473508, WTC...</td>
      <td>3633</td>
    </tr>
    <tr>
      <th>36</th>
      <td>GG</td>
      <td>NA</td>
      <td>CC</td>
      <td>[WTCCCT473436, WTCCCT469571, WTCCCT442728, WTC...</td>
      <td>4</td>
    </tr>
    <tr>
      <th>37</th>
      <td>GG</td>
      <td>NA</td>
      <td>CT</td>
      <td>[WTCCCT488883, WTCCCT474387, WTCCCT470552, WTC...</td>
      <td>88</td>
    </tr>
    <tr>
      <th>38</th>
      <td>GG</td>
      <td>NA</td>
      <td>NA</td>
      <td>[WTCCCT474593, WTCCCT469989, WTCCCT515392, WTC...</td>
      <td>61</td>
    </tr>
    <tr>
      <th>39</th>
      <td>GG</td>
      <td>NA</td>
      <td>TT</td>
      <td>[WTCCCT473549, WTCCCT489615, WTCCCT489614, WTC...</td>
      <td>270</td>
    </tr>
    <tr>
      <th>40</th>
      <td>GG</td>
      <td>TT</td>
      <td>CC</td>
      <td>[WTCCCT489604, WTCCCT467785, WTCCCT489030, WTC...</td>
      <td>91</td>
    </tr>
    <tr>
      <th>41</th>
      <td>GG</td>
      <td>TT</td>
      <td>CT</td>
      <td>[WTCCCT473454, WTCCCT489616, WTCCCT489608, WTC...</td>
      <td>872</td>
    </tr>
    <tr>
      <th>42</th>
      <td>GG</td>
      <td>TT</td>
      <td>NA</td>
      <td>[WTCCCT473540, WTCCCT473520, WTCCCT473486, WTC...</td>
      <td>552</td>
    </tr>
    <tr>
      <th>43</th>
      <td>GG</td>
      <td>TT</td>
      <td>TT</td>
      <td>[WTCCCT473530, WTCCCT473555, WTCCCT473426, WTC...</td>
      <td>2267</td>
    </tr>
  </tbody>
</table>
</div>



We can compute basic information about these 3 SNPs

```python
print("how many samples have at least one low quality (`NA`) genotype?", triple_SNPs.num_samples_NA)
print("how many samples have genotypes of high quality for all 3 SNPs?",triple_SNPs.total_samples_no_NA)
```

    how many samples have at least one low quality (`NA`) genotype? 2460
    how many samples have genotypes of high quality for all 3 SNPs? 12487


    /Users/ahoang/Documents/Learning/corradin_ovp_utils/.venv/lib/python3.8/site-packages/ipykernel/ipkernel.py:283: DeprecationWarning: `should_run_async` will not call `transform_cell` automatically in the future. Please pass the result to `transformed_cell` argument and any exception that happen during thetransform in `preprocessing_exc_tuple` in IPython 7.17 and above.
      and should_run_async(code)


We can query based on genotype of each SNP:

```python
triple_SNPs.query(rs77948203= "AA", rs9610458 = "CT")
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>rs77948203</th>
      <th>rs9610458</th>
      <th>rs134490</th>
      <th>unique_samples_id</th>
      <th>unique_samples_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>3</th>
      <td>AA</td>
      <td>CT</td>
      <td>CT</td>
      <td>[WTCCCT466178, WTCCCT468665, WTCCCT471002, WTC...</td>
      <td>8</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AA</td>
      <td>CT</td>
      <td>NA</td>
      <td>[WTCCCT474394, WTCCCT470548, WTCCCT443601]</td>
      <td>3</td>
    </tr>
    <tr>
      <th>5</th>
      <td>AA</td>
      <td>CT</td>
      <td>TT</td>
      <td>[WTCCCT470264, WTCCCT449002, WTCCCT467316, WTC...</td>
      <td>23</td>
    </tr>
  </tbody>
</table>
</div>



---

### The problem of multiple possible input types

The genetic file can be specified in multiple ways:
- Different formats (.gen, .bgen)
- Split into multiple files for different phenotypes (case/control etc)
- Split into one file per chromosome

Also, the input files can be:
- Stored in local disk
- In a compute cluster
- In the cloud

The library allows any combinations of these options. Let's look at the data catalog to see examples of these:

```python
#collapse_output closed

#printing out the catalog
!cat conf/base/catalog_input/genetic_file.yaml
```

    _MS_gen_file: &MS_gen_file
        type: corradin_ovp_utils.datasets.OVPDataset.OVPDataset
        file_format: genetic_file.GenFileFormat
        load_args:
            prob_n_cols: 3
            initial_cols:
                - "dashes"
                - "rsid"
                - "position"
                - "ref"
                - "alt"
            rsid_col: "rsid"
            ref_col: "ref"
            alt_col: "alt"
            pandas_args:
                sep: " "
                header: null
                
            
    
    genetic_file:
        <<: *MS_gen_file
        file_type: OVPDataset.CaseControlFilePathSchema
        file_path:
            case:
                folder: "data/test_data/gen_file"
                full_file_name: "test_CASE_MS_chr22.gen"
            control:
                folder: "data/test_data/gen_file"
                full_file_name: "test_CONTROL_MS_chr22.gen"
    
                
    genetic_file_common_folder:
        <<: *MS_gen_file
        file_type: OVPDataset.CaseControlFilePathSchema
        file_path:
            common_folder: "data/test_data/gen_file"
            case:
                full_file_name: "test_CASE_MS_chr22.gen"
            control:
                full_file_name: "test_CONTROL_MS_chr22.gen"
    
    
    genetic_file_single:
        <<: *MS_gen_file
        file_type: OVPDataset.SingleFilePathSchema
        file_path:
            folder: "data/test_data/gen_file"
            full_file_name: "test_CASE_MS_chr22.gen"
    
    
    genetic_file_split_by_chrom:
        <<: *MS_gen_file
        file_type: OVPDataset.CaseControlFilePathSchema
        file_path:
            common_folder: "data/test_data/gen_file"
            case:
                split_by_chromosome: True
                full_file_name: "test_CASE_MS_chr{chrom_num}.gen"
            control:
                split_by_chromosome: True
                full_file_name: "test_CONTROL_MS_chr{chrom_num}.gen"

```python
!cat conf/base/catalog_input/sample_file.yaml
```

    sample_file:
        type: corradin_ovp_utils.datasets.OVPDataset.OVPDataset
        file_type: OVPDataset.CaseControlFilePathSchema
        file_format: sample_file.SampleFileFormat
        load_args:
            sample_id_col: "ID_2"
            cov_cols: ["sex"]
            missing_col: "missing"
            pandas_args:
                sep: " "
                skiprows: [1] #2nd line of file is extra and should be discarded
        file_path:
            common_folder: "data/test_data/sample_file"
            case:
                full_file_name: "MS_impute2_ALL_sample_out.tsv"
            control:
                full_file_name: "ALL_controls_58C_NBS_WTC2_impute2_sample_out.tsv"
