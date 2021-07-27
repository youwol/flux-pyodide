import { getUrlBase, parseResourceId } from '@youwol/cdn-client';
import { AUTO_GENERATED as DF_AUTO_GENERATED} from '@youwol/dataframe';
import { FluxPack, IEnvironment } from '@youwol/flux-core'
import { forkJoin } from 'rxjs';
import { tap } from 'rxjs/operators';
import { AUTO_GENERATED } from '../auto_generated'
import { exposePythonUtilities, findDataframePaths, outputPython2Js, recoverDataFrames } from './utilities';

export var pyodide: any

export function install(environment: IEnvironment) {

    let indexPyodide = getUrlBase("@pyodide/pyodide", "0.17.0") + "/full"
    let dataframeResourceId = `@youwol/dataframe#${DF_AUTO_GENERATED.version}~dist/@youwol/dataframe.js`
    let pyodideResourceId =  `@pyodide/pyodide#0.17.0~full/pyodide.js` 

    let mainThread$ = window['loadPyodide']({indexURL:indexPyodide})
    .then( (py) => {
        pyodide = py 
        exposePythonUtilities(window, pyodide)
    })
    
    let workersThread$ = environment.fetchSources([
        parseResourceId(dataframeResourceId),
        parseResourceId(pyodideResourceId)
    ])
    .pipe(
        tap( (assets) => {
            environment.workerPool.import({
                sources: [
                    {
                        id: dataframeResourceId,
                        src: assets[0].content,
                        import: (workerScope, src) => {
                            new Function(src)( workerScope, undefined)
                        }
                    },
                    {
                        id: pyodideResourceId,
                        src: assets[1].content,
                        sideEffects: (workerScope, exports) => {
                            let indexUrl = workerScope['@youwol/flux-pyodide.indexPyodide']
                            return workerScope
                            .loadPyodide({indexURL:`${workerScope.location.origin}${indexUrl}`})
                            .then( (pyodide) => {
                                let exposePythonUtilities = workerScope['@youwol/flux-pyodide.exposePythonUtilities']
                                exposePythonUtilities(workerScope, pyodide)
                            })
                        }
                    }
                ],
                functions:[
                    {
                        id:'@youwol/flux-pyodide.exposePythonUtilities', 
                        target:exposePythonUtilities
                    },
                    {
                        id:'@youwol/flux-pyodide.findDataframePaths', 
                        target:findDataframePaths
                    },
                    {
                        id:'@youwol/flux-pyodide.outputPython2Js', 
                        target:outputPython2Js
                    },
                    {
                        id:'@youwol/flux-pyodide.recoverDataFrames', 
                        target:recoverDataFrames
                    }
                ],
                variables:[
                    {
                        id:'@youwol/flux-pyodide.indexPyodide',
                        value:indexPyodide
                    }
                ]
            })
        })
    )

    return forkJoin([mainThread$, workersThread$])
}

export let pack = new FluxPack({
    ...AUTO_GENERATED,
    ...{
        install
    }
})

