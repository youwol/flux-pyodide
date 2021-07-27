import { DataFrame, Serie } from "@youwol/dataframe";

export function exposePythonUtilities(scope: any, pyodide:any){

    pyodide.globals.set(
        "create_yw_dataframe", 
        (name, series) =>  {
            let DataframeModule = scope["@youwol/dataframe"]
            let s = Object.fromEntries(series.toJs().entries());
            return DataframeModule.DataFrame.create( { 
                series: s, 
                userData: {
                    name,
                } }) 
        }
    )
    pyodide.globals.set(
        "create_yw_serie", 
        (array, itemSize) =>  {
            let DataframeModule = scope["@youwol/dataframe"]
            // to think about: using array.getBuffer().data (TypedArray)
            return DataframeModule.Serie.create( { 
                array: array.toJs(), 
                itemSize 
            }) 
        }
    );
    pyodide.runPython(`
def dataframe_pd_to_yw(name, df):
    series = {}
    for column in df:
        series[column] = create_yw_serie(df[column].values, 1)
    yw_df = create_yw_dataframe(name, series)
    return yw_df

def serie_yw_to_np(serie):

    item_size = serie.itemSize 
    array = np.array(serie.array.to_py())
    if item_size != 1:        
        x_count = int(serie.array.length/item_size)
        array = array.reshape((x_count,item_size))
    return array, item_size            

def serie_yw_to_pd(serie):
    array, item_size = serie_yw_to_np(serie)
    if item_size == 1:
        return pd.Series(data=array)
    return pd.Series(data=array.tolist())

def dataframe_yw_to_pd(df, columns = None):
    series = {}
    for k in df.series.to_py():
        if columns and k not in columns:
            continue
        serie = df.series.to_py()[k]
        series[k] = serie_yw_to_pd(serie)
    return pd.DataFrame(series)
`)     
}

export function outputPython2Js(d) {
    let recFct = (d) => {
        if (d instanceof Map) {
            let converted = {}
            d.forEach((v, k) => {
                converted[k] = recFct(v)
            })
            return converted
        }
        return d
    }
    return recFct(d.toJs ? d.toJs() : d)
}


export function findDataframePaths(workerScope, d, parentPath = "") {

    if (d instanceof workerScope['@youwol/dataframe'].DataFrame)
        return [parentPath + '/']

    if (typeof(d)=='object'){
        let paths = Object.entries(d).map(([k, v]) => {
            return findDataframePaths(workerScope, v, parentPath + "/" + k)
        })
        return paths.flat()
    }
    return []
}


export function recoverDataFrames(workerScope, data, dataframePaths){

    let dataframeModule = workerScope['@youwol/dataframe']

    dataframePaths.forEach( path => {

        let [ref, key, parent] =  path
        .split('/')
        .slice(1,-1)
        .reduce( ([ref, , ], pathElem) => {
            return [ref[pathElem], pathElem, ref]
        }, [data, , ])
        
        let series = Object
        .entries(ref.series)
        .map( ([k,serie]) => {
            return [k,dataframeModule.Serie.create(serie as any)]
        })
        .reduce( (acc, [k, serie]:[k:string, serie: Serie]) =>{
            acc[k]=serie
            return acc
        } , {})
        let df = dataframeModule.DataFrame.create({...ref, ...{series} })
        if(parent)
            parent[key] = df
        else
            data = df
    })
    return data
}