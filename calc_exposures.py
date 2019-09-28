import json
from QuantLib import *
import numpy as Numpy
import boto3
import os
import csv
from utils import *

global_cache = {}

def lambda_handler(event, context):
    
    calendar = TARGET()
    startDate = Date(12, December, 2018)
    settlementDate = Date(14, December, 2018)
    endDate = Date(14, December, 2023)
    gridStepPeriod = Period(1, Weeks)
    firstIndexFixing = 0.0277594
    a = 0.1421842834
    sigma = 0.0081355969
    Settings.instance().evaluationDate = startDate
    
    clear_output_bucket()

    # request simulation grid, define times and dates
    grid = Grid(settlementDate, endDate, gridStepPeriod)
    times = grid.GetTimes()
    dates = grid.GetDates()
    
    if ('market_curve' in global_cache):
        marketCurve = global_cache['market_curve']
    else:
        marketCurve = load_discount_curve()
        global_cache['market_curve'] = marketCurve
    
    # request Hull-White 1-factor process and calibrated model parameters
    # generate paths for short rate
    nPaths = 500
    process = HullWhiteProcess(YieldTermStructureHandle(marketCurve), a, sigma)
    
    #Generate paths, resulting array dimension: n, len(timeGrid)
    timeGrid = grid.GetTimeGrid()
    sequenceGenerator = UniformRandomSequenceGenerator(len(timeGrid), UniformRandomGenerator())
    gaussianSequenceGenerator = GaussianRandomSequenceGenerator(sequenceGenerator)
    maturity = timeGrid[len(timeGrid) - 1]
    pathGenerator = GaussianPathGenerator(process, maturity, len(timeGrid), gaussianSequenceGenerator, False)
    paths = Numpy.zeros(shape=(nPaths, len(timeGrid)))
    for i in range(nPaths):
        path = pathGenerator.next().value()
        paths[i, :] = Numpy.array([path[j] for j in range(len(timeGrid))])

    forecastingCurve = RelinkableYieldTermStructureHandle()
    index = USDLibor(Period(3, Months), forecastingCurve)
    transaction = CreateSwapTransaction(index)
    
    # create fixing dates, request simulated fixings
    # correction for the first observed fixing
    simulatedFixingDates = Numpy.array(dates) - Period(index.fixingDays(), Days)
    simulatedFixingRates = Numpy.mean(paths[:], axis=0)
    simulatedFixingRates[0] = firstIndexFixing
    
    s3 = boto3.client('s3')
    bucket = os.environ['BUCKET_OUT']
    sim_fixings_file = os.environ['SIMULATED_FIXINGS_FILE']
    s3 = boto3.resource('s3')
    s3.Object(bucket, sim_fixings_file).delete()
    csv_line='date,fixing\n'
    for i in range(simulatedFixingDates.shape[0]):
        csv_line = csv_line + ql_to_pydate(simulatedFixingDates[i]).strftime('%m/%d/%Y') + ',' + str(simulatedFixingRates[i]) + '\n'
    s3.Bucket(bucket).put_object(Key=sim_fixings_file, Body=csv_line.encode('utf-8'))
    
    # simulate exposures
    nPaths = 500
    session = boto3.session.Session(region_name='us-east-1')
    client_lambda = session.client('lambda')
    
    for s in range(nPaths):
        print('Simulation # : {}'.format(s))
        sim_event = dict([("simulation_num", s),
                          ("first_index_fixing", firstIndexFixing),
                          ("a", a),
                          ("sigma", sigma),
                          ("settlement_date", ql_to_pydate(settlementDate).strftime('%m/%d/%Y')),
                          ("end_date", ql_to_pydate(endDate).strftime('%m/%d/%Y')),
                          ("grid_step_period", "1W"),
                          ("one_path", paths[s, :].tolist())])
                          
        payload = json.dumps(sim_event)
        
        client_lambda.invoke(
            FunctionName='calc_one_path_exposure',
            InvocationType='Event',
            LogType='None',
            Payload=payload
        )
        
    print("All simulations submitted")