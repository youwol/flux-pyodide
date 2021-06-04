import { getUrlBase } from '@youwol/cdn-client';
import { DataFrame, Serie } from '@youwol/dataframe';
import { FluxPack, IEnvironment } from '@youwol/flux-core'
import { AUTO_GENERATED } from '../auto_generated'

export var pyodide: any

export function install(environment: IEnvironment) {
    let indexURL = getUrlBase("@pyodide/pyodide", "0.17.0") + "/full"

    return window['loadPyodide']({indexURL})
    .then( (py) => {
        pyodide = py 
        pyodide.globals.set(
            "createDataFrame", 
            (name, series) =>  {
                let s = Object.fromEntries(series.toJs().entries());
                return DataFrame.create( { series: s, userData:{name} }) 
            }
        )
        pyodide.globals.set(
            "createSerie", 
            (array, itemSize) =>  Serie.create( { array: array.toJs(), itemSize })
        );
        pyodide.runPython(`
def toNpArray(serie, flat = False):
    array = np.array(serie.array.to_py())
    if not flat:        
        x_count = int(serie.array.length/serie.itemSize)
        array = array.reshape((x_count,serie.itemSize))
    return array       

def toPdSerie(serie, flat = False):
    array = toNpArray(serie, flat)
    if flat:
        return pd.Series(data=array)
    return pd.Series(data=array.tolist())

def toPdDataFrame(df, columns, flat = False):
    series = {}
    for k in df.series.to_py():
        if k not in columns:
            continue
        serie = df.series.to_py().get(k)
        series[k] = toPdSerie(serie, flat)
    return pd.DataFrame(series)

`)     
    })
}

export let pack = new FluxPack({
    ...AUTO_GENERATED,
    ...{
        install
    }
})

