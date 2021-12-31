import streamlit as st
import pandas as pd
from corradin_ovp_utils.cli import run_ovp_pipeline
import os
import numpy as np
import altair as alt




class MultiPage: 
    """Framework for combining multiple streamlit applications."""

    def __init__(self) -> None:
        """Constructor class to generate a list which will store all our applications as an instance variable."""
        self.pages = []
    
    def add_page(self, title, func) -> None: 
        """Class Method to Add pages to the project
        Args:
            title ([str]): The title of page which we are adding to the list of apps 
            
            func: Python function to render this page in Streamlit
        """

        self.pages.append({
          
                "title": title, 
                "function": func
            })

    def run(self):
        # Drodown to select the page to run  
        page = st.sidebar.selectbox(
            'App Navigation', 
            self.pages, 
            format_func=lambda page: page['title']
        )
        # run the app function 
        page['function']()


st.title("Run OVP pipeline")

st.subheader("Provide dataset")
dataset = st.selectbox("What dataset to use?", options = ["MS", "test_MS", "UKB_default", "UKB_risk_taking"], help = "If the dataset you want are not available here, talk to An")

st.subheader("Provide file path or choose a file for pairs file")
pairs_file_path = st.text_input("File path:", value="/lab/corradin_biobank/FOR_AN/OVP/corradin_ovp_utils/test_MS_chr22.tsv", help="File should be in '.tsv' format, and include 4 columns: ['GWAS_id', 'outside_id', 'GWAS_chrom', outside_chrom'] ", placeholder="Type your pairs file path")
pairs_file_upload = st.file_uploader("Choose a file:")


output_folder = st.text_input("Output folder path:", value="/lab/corradin_biobank/FOR_AN/OVP/corradin_ovp_utils/new_pipeline_results", placeholder= "/lab/corradin_biobank/FOR_AN/OVP/corradin_ovp_utils/new_pipeline_results")

run_button = st.button("Run pipeline 🚀")

if run_button:
    st.markdown("Pipeline is running! Visit [this link](https://cloud.prefect.io/corradin-lab-mit) to view progress: ")

# def analyze_result_app():
#     import plotly
#     st.title("Analyze results")
#     st.subheader("Provide file path or choose a file for result file")
#     result_file_path = st.text_input("File path:", value="/lab/corradin_biobank/FOR_AN/OVP/174_loci_binarized_Sept18_2021_Whitehead_Retreat.tsv")
#     result_file_upload = st.file_uploader("Choose a file:")
#     st.subheader("Detailed result")
#     st.write(pd.read_csv("/lab/corradin_biobank/FOR_AN/OVP/174_loci_Sept2_2021_detailed_results.tsv", sep = "\t",nrows=20))
#     st.subheader("Binarized results")
#     result_df = pd.read_csv(result_file_path, sep = "\t")
#     st.write(result_df)
#     st.write(result_df[["GWAS_rsID", "outside_rsID"]].groupby(["GWAS_rsID"]).count())
    
#     # Add histogram data
#     x1 = np.random.randn(200) - 2
#     x2 = np.random.randn(200)
#     x3 = np.random.randn(200) + 2

#     # Group data together
#     hist_data = [x1, x2, x3]

#     group_labels = ['Group 1', 'Group 2', 'Group 3']
#     df = pd.DataFrame(
#      np.random.randn(200, 3),
#      columns=['a', 'b', 'c'])

#     c = alt.Chart(df).mark_circle().encode(
#      x='a', y='b', size='c', color='c', tooltip=['a', 'b', 'c'])
#     st.subheader("Example analysis graph")
#     st.altair_chart(c, use_container_width=True)

#     # Create distplot with custom bin_size
#     fig = plotly.figure_factory.create_distplot(
#              hist_data, group_labels, bin_size=[.1, .25, .5])

#     # Plot!
#     st.plotly_chart(fig, use_container_width=True)

# app = MultiPage()

# app.add_page("Run OVP pipeline", run_pipeline_app)
# app.add_page("Analyze results", analyze_result_app)

# app.run()
