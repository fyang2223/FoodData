import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

def makegraph(csv_loc):
    df = pd.read_csv(csv_loc,index_col=0)

    CarbInfo = df.loc[df['name'] == "Carbohydrate, by difference"] \
        .sort_values('avg(amount) per 100g', ascending=True) \
        .loc[df['sample_size'] > 10]
    CarbInfo = CarbInfo.reset_index()

    n = len(CarbInfo)
    fig, ax = plt.subplots(figsize=(10, n/3))

    cont = ax.barh(CarbInfo.index, CarbInfo['avg(amount) per 100g'], align='center')

    ax.set_yticks(np.arange(n))
    ax.set_yticklabels(CarbInfo['branded_food_category'].values)
    ax.set_ylim(-0.5, n)

    ax.set_xlabel('avg(amount) per 100g')
    ax.bar_label(cont, labels=CarbInfo['sample_size'].values)
    ax.set_title('Carbohydrate Content in USDA-listed Foods\n(Bar Labels: Sample Size)')

    ax.grid(axis='x')

    fig.savefig('data/CarbohydrateContent.png', bbox_inches="tight")

