import os
import logging
import pandas as pd
from itertools import combinations
from bs4 import BeautifulSoup
import pdfkit
import json 

from sdmetrics.reports.single_table import QualityReport as QRS
from sdmetrics.reports.multi_table import QualityReport as QRM
from sdmetrics.reports.single_table import DiagnosticReport as DRS
from sdmetrics.single_table import StatisticSimilarity, CategoricalKNN, NumericalMLP, MissingValueSimilarity, CategoricalCAP
from sdmetrics.reports.utils import get_column_plot
from sdmetrics.reports.utils import get_column_pair_plot

logger = logging.getLogger(__name__)
data_folder = '../../../data/proj_demo_synthST/airflow_data/report/'

def filter_is_csv(item):
    """
    Returns true if the item is a csv file
    """
    return item.endswith(".csv")

def filter_is_json(item):
    """
    Returns true if the item is a csv file
    """
    return item.endswith(".json")

def get_data(input_folder):
    """
    Get the aggregated dataframe, consisting of all data inside 'input_folder'
    
    Args:
       input_folder (str): specifying the folder which stores the data
    Returns:
       pd.DataFrame: aggregated data frame in a dictionary and list
       with the name of the matadata json file (can be an empty list) 
    """
    
    list_of_files = []

    logger.info(f"Reads csv files from nested folder {input_folder}")

    for (dirpath, dirnames, filenames) in os.walk(input_folder):
        list_of_files += [os.path.join(dirpath, file) for file in filenames]

    logger.info(f"loaded {len(list_of_files)} files")

    # Make relative paths from absolute file paths
    rel_paths = [os.path.relpath(item, input_folder) for item in list_of_files]
    logger.info(rel_paths)
    
    # Filter out csv files
    json_path_files = list(filter(filter_is_json, rel_paths))
    # Handling cases in which the metadata file is not provided
    if json_path_files:
        json_path = list(filter(filter_is_json, rel_paths))[0]
    else:
        json_path = []

    # Filter out non-csv files
    rel_paths = list(filter(filter_is_csv, rel_paths))
    logger.info(rel_paths)
    # Get absolute paths
    abs_paths = [os.path.join(input_folder, item) for item in rel_paths]
    # Concatenate all files in a dictionary
    dfs = {}

    for item,file in zip(rel_paths,abs_paths):
        dfs[item[:-4]] = pd.read_csv(file, encoding = 'ISO-8859-1')
    logger.info(dfs.keys())
    return dfs,json_path

def generate_pdfReports(path):
    partNames = ["Quality Report", "Diagnostic Report", "Other Scores"]
    subPartNamesQuality = ["Column Shape", "Column Pair Trends"]
    subPartNamesDiagnostic = ["Synthesis", "Coverage", "Boundaries"]
    subPartNamesOther = ["Scores"]
    subPartNames = [subPartNamesQuality, subPartNamesDiagnostic, subPartNamesOther]

    parts = ["overallQualityReport.html", "overallDiagReport.html", "scores.html"]
    subPartsQuality = ["columnShapeReport.html", "columnPairsReport.html"]
    subPartsDiagnostics = ["synthesisReport.html", "coverageReport.html", "boundariesReport.html"]
    subPartsScores = ["scores.html"]
    subParts = [subPartsQuality, subPartsDiagnostics, subPartsScores]

    output_doc_man = BeautifulSoup('<html></html>', 'html.parser')
    output_doc_man.html.append(output_doc_man.new_tag("body"))
    title = output_doc_man.new_tag("h1")
    title.string = "Metrics Report"
    output_doc_man.body.append(title)

    for i, part in enumerate(parts):
        partName = output_doc_man.new_tag("h2")
        partName.string = partNames[i]
        output_doc_man.body.append(partName)

        if i == 1:
            with open(os.path.join(path, "diagnosticReportResult.json"), 'r') as json_file:
                diagnosticReportResult = json.load(json_file)
                for key in diagnosticReportResult:
                    subPartName = output_doc_man.new_tag("p")
                    subPartName.string = str(key) + ": " + str(diagnosticReportResult[key])
                    output_doc_man.body.append(subPartName)
            
        with open(os.path.join(path, part), 'r') as html_file:
            output_doc_man.body.append(BeautifulSoup(html_file.read(), "html.parser"))

        for j, subPart in enumerate(subParts[i]):
            if subPartNames[i][j] != "Scores":
                subPartName = output_doc_man.new_tag("h3")
                subPartName.string = subPartNames[i][j]
                output_doc_man.body.append(subPartName)

                with open(os.path.join(path, subPart), 'r') as html_file:
                    output_doc_man.body.append(BeautifulSoup(html_file.read(), "html.parser"))
    
    with open(os.path.join(path, "allScores.html"), "w", encoding = 'utf-8') as html: 
        html.write(str(output_doc_man.prettify()))
    
    pdfkit.from_file(os.path.join(path,"allScores.html"), os.path.join(path,"allScores.pdf"), options={"enable-local-file-access": ""})

def generate_pdfVisualizations(path):
    partNames = ["Quality Report", "Diagnostic Report", "Other Visualizations"]
    subPartNamesQuality = ["Column Shape", "Column Pair Trends"]
    subPartNamesDiagnostic = ["Synthesis", "Coverage", "Boundaries"]
    subPartNamesOther = ["Column Pair Distribution", "Column Distribution"]
    subPartNames = [subPartNamesQuality, subPartNamesDiagnostic, subPartNamesOther]

    subPartsQuality = ["columnShape.png", "columnPair.png"]
    subPartsDiagnostics = ["synthesis.png", "coverage.png", "boundaries.png"]
    subPartsOther = ["columnPairDist.png", "columnDist.png"]
    subParts = [subPartsQuality, subPartsDiagnostics, subPartsOther]

    png_doc_man = BeautifulSoup('<html></html>', 'html.parser')
    png_doc_man.html.append(png_doc_man.new_tag("body"))
    title = png_doc_man.new_tag("h1")
    title.string = "Visualizations"
    png_doc_man.body.append(title)

    for i in range(len(partNames)):
        partName = png_doc_man.new_tag("h2")
        partName.string = partNames[i]
        png_doc_man.body.append(partName)

        for j in range(len(subPartNames[i])):
            subPartName = png_doc_man.new_tag("h3")
            subPartName.string = subPartNames[i][j]
            png_doc_man.body.append(subPartName)

            with open(os.path.join(path, subParts[i][j]), 'r') as file:
                png_doc_man.body.append(png_doc_man.new_tag('img', src=os.path.join(path,subParts[i][j])))
    
    with open(os.path.join(path, "allVisualizations.html"), "w", encoding = 'utf-8') as html: 
        html.write(str(png_doc_man.prettify()))
    
    pdfkit.from_file(os.path.join(path,"allVisualizations.html"), os.path.join(path,"allVisualizations.pdf"), options={"enable-local-file-access": ""})

def get_metrics(**context):

    # Create setup variable
    main_data_dir = "{}".format(context["dag_run"].conf["main_data_dir"])
    project_dir = main_data_dir + '/' + "{}".format(context["dag_run"].conf["project_dir"])
    dataset_type = "{}".format(context["dag_run"].conf["dataset_type"])

    logger.info(project_dir)

    # Get synthetic and orginal data
    tables_orig, metadataFileOrig = get_data(project_dir + '/airflow_data/data_orig')
    tables_synth, metadataFileSynth = get_data(project_dir + '/airflow_data/data_synth')
    
    # Retreive table names
    tableNames = list(tables_orig.keys()) 
    
    # Load metadata
    with open(project_dir + '/airflow_data/data_orig/' + metadataFileOrig) as f:
        metadata = json.load(f)
        logger.info(metadata)

    if dataset_type == 'singleTable':
        logger.info(metadata)

        dataOrig = tables_orig[tableNames[0]]
        dataSynth = tables_synth[tableNames[0]]
        colNames = dataOrig.columns.tolist()
        
        # Generate quality report
        qualityReport = QRS()
        qualityReport.generate(dataOrig, dataSynth, metadata)
        qualityReport.save(filepath=os.path.join(project_dir, "airflow_data/report/qualityReport.pkl"))

        # Overall QualityReport 
        overallScore = qualityReport.get_properties()
        overallScore.to_html(os.path.join(project_dir, "airflow_data/report/overallQualityReport.html"))

        # QualityReport Property "Column Shapes"
        columnsShape = qualityReport.get_details(property_name='Column Shapes')
        columnsShape.to_html(os.path.join(project_dir, "airflow_data/report/columnShapeReport.html"))

        figColumnShape = qualityReport.get_visualization(property_name='Column Shapes')
        figColumnShape.write_image(os.path.join(project_dir, "airflow_data/report/columnShape.png"))

        # QualityReport Property "Column Pair Trends"
        columnPairs = qualityReport.get_details(property_name='Column Pair Trends')
        columnPairs.to_html(os.path.join(project_dir, "airflow_data/report/columnPairsReport.html"))

        figColumnPairs = qualityReport.get_visualization(property_name='Column Pair Trends')
        figColumnPairs.write_image(os.path.join(project_dir, "airflow_data/report/columnPair.png"))

        # Generate Diagnostic Report
        diagnosticReport = DRS()
        diagnosticReport.generate(dataOrig, dataSynth, metadata)
        diagnosticReport.save(filepath=os.path.join(project_dir, "airflow_data/report/diagnosticReport.pkl"))

        # Diagnostic Report Result
        diag_json = json.dumps(diagnosticReport.get_results())
        with open(os.path.join(project_dir, "airflow_data/report/diagnosticReportResult.json"), 'w') as f:
            f.write(diag_json)
        
        # Diagnostic Report Overall Result
        diagOverall = diagnosticReport.get_properties()
        dfdiagOverall = pd.DataFrame.from_dict(diagOverall, orient="index")
        dfdiagOverall.to_html(os.path.join(project_dir, "airflow_data/report/overallDiagReport.html"))

        # Diagnostic Report Property "Synthesis"
        synthesis = diagnosticReport.get_details(property_name='Synthesis')
        synthesis.to_html(os.path.join(project_dir, "airflow_data/report/synthesisReport.html"))

        figSynthesis = diagnosticReport.get_visualization(property_name='Synthesis')
        figSynthesis.write_image(os.path.join(project_dir, "airflow_data/report/synthesis.png"))

        # Diagnostic Report Property "Coverage"
        coverage = diagnosticReport.get_details(property_name='Coverage')
        coverage.to_html(os.path.join(project_dir, "airflow_data/report/coverageReport.html"))

        figCoverage = diagnosticReport.get_visualization(property_name='Coverage')
        figCoverage.write_image(os.path.join(project_dir, "airflow_data/report/coverage.png"))

        # Diagnostic Report Property "Boundaries"
        boundaries = diagnosticReport.get_details(property_name='Boundaries')
        boundaries.to_html(os.path.join(project_dir, "airflow_data/report/boundariesReport.html"))

        figBoundaries = diagnosticReport.get_visualization(property_name='Boundaries')
        figBoundaries.write_image(os.path.join(project_dir, "airflow_data/report/boundaries.png"))

        # Column Pair Distribution Visualization
        figColumnPair = get_column_pair_plot(
            real_data=dataOrig,
            synthetic_data=dataSynth,
            metadata = metadata,
            column_names=[colNames[3],colNames[4]]
        )

        figColumnPair.write_image(os.path.join(project_dir, "airflow_data/report/columnPairDist.png"))

        # Column Pair Trend Visualization
        fig = get_column_plot(
            real_data=dataOrig,
            synthetic_data=dataSynth,
            column_name=colNames[3],
            metadata=metadata
        )

        fig.write_image(os.path.join(project_dir, "airflow_data/report/columnDist.png"))
        

        statisticSimilarityMean = StatisticSimilarity.compute(
            real_data=dataOrig,
            synthetic_data=dataSynth,
            statistic='mean'
        )

        statisticSimilarityMedian = StatisticSimilarity.compute(
            real_data=dataOrig,
            synthetic_data=dataSynth,
            statistic='median'
        )

        statisticSimilarityStd = StatisticSimilarity.compute(
            real_data=dataOrig,
            synthetic_data=dataSynth,
            statistic='std'
        )

        missingValueSimilarity = MissingValueSimilarity.compute(
            real_data=dataOrig,
            synthetic_data=dataSynth
        )
        
        categoricalCap = CategoricalCAP.compute(
            real_data=dataOrig.dropna()[:10000],
            synthetic_data=dataSynth.dropna()[:10000],
            key_fields=[colNames[4],colNames[3]],
            sensitive_fields=[colNames[8]]
        )
        
        scores = pd.DataFrame.from_dict({
            "statisticSimilarity (mean)": [statisticSimilarityMean], 
            "statisticSimilarity (median)": [statisticSimilarityMedian],
            "statisticSimilarity (standard deviation)": [statisticSimilarityStd],
            "categoricalCAP (operation, type) -> bank": [categoricalCap],
            "missingValueSimilarity": [missingValueSimilarity],
            })
            
        scores.to_html(os.path.join(project_dir, "airflow_data/report/scores.html"))

        generate_pdfReports(project_dir + '/airflow_data/report')
        generate_pdfVisualizations(project_dir + '/airflow_data/report')

    elif dataset_type == 'multiTable':
        logger.info(metadata)
        # Genrate quality report
        qualityReport = QRM()
        qualityReport.generate(tables_orig, tables_synth, metadata)
        qualityReport.save(filepath=os.path.join(project_dir, "airflow_data/report/report.pkl"))

    else:        
        logger.info('The dataset is not supported.') 
