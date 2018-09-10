from sklearn.metrics import r2_score
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

def rmse(x, y):
    return np.sqrt(np.mean((x-y)**2))
def rmse_score(cls, x, y):
    return rmse(cls.predict(x), y)

def show_prediction_stats(pred):
    print('col(count) stats: ', pred['count'].describe(), end='\n\n')
    print('scaled RMSE: ', rmse(pred['count_scaled'], pred['pred_scaled']), end='\n\n')
    print('RMSE: ', rmse(pred['count'], pred['pred']), end='\n\n')
    print('R^2 :', r2_score(pred['count'], pred['pred']), end='\n\n')
    pred['residual'] = pred['pred'] - pred['count']
    print('Residual stats: ', pred.residual.describe())
    fig, ax = plt.subplots(figsize=(12,8))
    pred.plot.scatter('count', 'residual', ax=ax)

# check: if pred ~= max(count) * pred_scaled, then rmse_scaled * max(count) = rmse
def check_count_scaling_properties(pred):
    max_count = pred['count'].max()
    print('Max abs difference - scaled prediction vs prediction:',
            (max_count * pred_train['pred_scaled'] - pred_train['pred']).abs().max())
    print('scaled RMSE vs RMSE:', 
            rmse_scaled_train * max_count, rmse_train)
