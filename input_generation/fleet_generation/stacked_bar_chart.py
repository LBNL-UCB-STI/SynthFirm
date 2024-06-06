# Prepare data
df_melted = df.melt(id_vars=['avgSpeedBinDesc', 'Source'],
                    value_vars=['Combination Long-haul Truck', 'Combination Short-haul Truck', 'Light Commercial Truck', 'Single Unit Long-haul Truck', 'Single Unit Short-haul Truck'],
                    var_name='truck_type', value_name='emission_rate')

# Order speed bins
desired_order = [
    'speed < 2.5mph', '2.5mph <= speed < 7.5mph', '7.5mph <= speed < 12.5mph',
    '12.5mph <= speed < 17.5mph', '17.5mph <= speed < 22.5mph', '22.5mph <= speed < 27.5mph',
    '27.5mph <= speed < 32.5mph', '32.5mph <= speed < 37.5mph', '37.5mph <= speed < 42.5mph',
    '42.5mph <= speed < 47.5mph', '47.5mph <= speed < 52.5mph', '52.5mph <= speed < 57.5mph',
    '57.5mph <= speed < 62.5mph', '62.5mph <= speed < 67.5mph', '67.5mph <= speed < 72.5mph',
    '72.5mph <= speed'
]

# Pivot
df_melted['avgSpeedBinDesc'] = pd.Categorical(df_melted['avgSpeedBinDesc'], categories=desired_order, ordered=True)
df_pivot = df_melted.pivot_table(index=['avgSpeedBinDesc', 'Source'], columns='truck_type', values='emission_rate', fill_value=0).reset_index()

df_pivot.sort_values('avgSpeedBinDesc', inplace=True)
speed_bins = df_pivot['avgSpeedBinDesc'].unique()
sources = df_pivot['Source'].unique()
truck_types = df_pivot.columns[2:]  # exclude 'avgSpeedBinDesc' and 'Source'

bar_width = 0.35
n_sources = len(sources)
total_width = bar_width * n_sources

def plot_data(normalized=True):
    plt.figure(figsize=(24, 15))
    plt.rcParams.update({'font.size': 20})

    # Colors for truck types and sources
    truck_colors = plt.cm.Paired(np.linspace(0, 1, len(truck_types)))
    source_colors = plt.cm.tab10(np.linspace(0, 1, n_sources))

    # Normalize the data
    if normalized:
        df_pivot_norm = df_pivot.copy()
        df_pivot_norm[truck_types] = df_pivot[truck_types].div(df_pivot[truck_types].sum(axis=1), axis=0) * 100
    else:
        df_pivot_norm = df_pivot

    # Plot
    for j, source in enumerate(sources):
        source_df = df_pivot_norm[df_pivot_norm['Source'] == source]
        truck_bottom = np.zeros(len(speed_bins))
        x_positions = np.arange(len(speed_bins)) + j * bar_width

        for i, truck in enumerate(truck_types):
            plt.bar(x_positions, source_df[truck].values, width=bar_width, bottom=truck_bottom, color=truck_colors[i], align='center', label=f'{source} - {truck}' if j == 0 else "")
            truck_bottom += source_df[truck].values

    if normalized:
        # Add total values
        for j, source in enumerate(sources):
            for i, sb in enumerate(speed_bins):
                total_rate = df_pivot[df_pivot['Source'] == source].iloc[i][truck_types].sum()
                plt.text(i + j * bar_width, truck_bottom[i] * 1.02, f'{total_rate:,.2f}', ha='center', va='bottom', rotation=90, fontsize=14)

    plt.xlabel('Speed Bin')
    plt.ylabel('Percentage of Total Emission Rate' if normalized else 'Total Emission Rate')
    plt.title(f'Distribution of CO Emission Rate by Speed Bin, Source, and Truck Type\nSources: {sources[0]} (Left), {sources[1]} (Right) - ' + ('Normalized' if normalized else 'Absolute Values'))
    plt.xticks(np.arange(len(speed_bins)) + total_width / 2 - bar_width / 2, speed_bins, rotation=45, ha='right')

    # Legend
    truck_handles = [plt.Rectangle((0,0),1,1, color=truck_colors[i]) for i in range(len(truck_types))]
    plt.legend(handles=truck_handles, labels=truck_types.tolist(), title='Truck Type', loc='upper left', bbox_to_anchor=(1, 1))

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.grid(True, which="both", ls="--")
    plt.ylim(0, 110 if normalized else truck_bottom.max() * 1.1)
    plt.savefig(f'CO_Emission_Rate_by_Source_{"Normalized" if normalized else "Absolute"}.png')
    plt.show()

plot_data(normalized=True)
plot_data(normalized=False)
