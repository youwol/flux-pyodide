import { Context, expect, BuilderView, Flux, Schema, ModuleError, Contract
} from '@youwol/flux-core'

import{pack,} from './main'
import { InterpreterBase } from './interpreter-base.module'
import { filter, take } from 'rxjs/operators'
import { WorkerContext } from '@youwol/flux-core/src/lib/worker-pool'
import { findDataframePaths, recoverDataFrames } from './utilities'


/**
  ## Presentation


 ## Resources
 [Pyodide 0.17](https://hacks.mozilla.org/2021/04/pyodide-spin-out-and-0-17-release/)
 [Pyodide FAQ](https://pyodide.org/en/latest/usage/faq.html)

 */
export namespace ModuleInterpreterWorker{

    interface WorkerArguments {
        script: string
        packages: Array<string>
        data: any
        outputsCount: number,
        dataframePaths: Array<string>
    }

    export function interpretInWorker( { args, taskId, context, workerScope }:{
        args: WorkerArguments, 
        taskId: string,
        workerScope: any,
        context: WorkerContext
    }) {
        let pyodide = workerScope.pyodide
        let findDataframePaths = workerScope['@youwol/flux-pyodide.findDataframePaths']
        let outputPython2Js = workerScope['@youwol/flux-pyodide.outputPython2Js']
        let recoverDataFrames = workerScope['@youwol/flux-pyodide.recoverDataFrames']

        let packagesToLoad = args.packages
        .filter( pack => !pyodide.loadedPackages[pack])
        context.info(`Load packages ${packagesToLoad} in worker`)
        
        let outputs = Array.from({length: args.outputsCount}, (v, i) => {
            return {
                next: (d) => { 
                    let data = outputPython2Js(d)
                    let dataframePaths = findDataframePaths(workerScope, data)
                    context.info(`Send in output ${i}`, { data , dataframePaths})
                    context.sendData({
                        outputIndex:i, 
                        data, 
                        dataframePaths
                    })
                }
            }
        }) 

        return Promise.all(
            packagesToLoad
            .map( pack => {
                // that would be great to have access to the request's loading progress
                return pyodide.loadPackage(pack).then( () => {
                    context.info(`Package ${pack} loaded successfully`)
                })
            })
        ).then( () => {
            let inputData = recoverDataFrames(workerScope, args.data, args.dataframePaths)
            context.info(`Run script`, {dataframePaths: args.dataframePaths, data: args.data})
            let fct = workerScope.pyodide.runPython(args.script)
            return fct(inputData, context, outputs)
        })    
    }


    /**
     * ## Persistent Data  🔧
     *
     */
    @Schema({
        pack
    })
    export class PersistentData extends InterpreterBase.PersistentData{

        constructor(d) {
            super(d)
        }
    }
    let transferabilityExpectation =  expect({
        description:"The data is transferable in worker (json & SharedArrayBuffer)",
        when: (data) => {
            try{
                let worker = new Worker(URL.createObjectURL(new Blob([''], { type: 'text/javascript' })))
                worker.postMessage(data)
                return true
            }
            catch(e){
                return false 
            }
        }
    })
    /*
    let inputContract = new Contract(
        "The data is transferable in worker (json & SharedArrayBuffer)",
        {transferabilityExpectation}
    ) */

    /** ## Module
     **/
    @Flux({
        pack: pack,
        namespace: ModuleInterpreterWorker,
        id: "ModuleInterpreterWorker",
        displayName: "Python Worker",
        description: "Python interpreter in worker",
        resources: {
            'technical doc': `${pack.urlCDN}/dist/docs/modules/`
        }
    })
    @BuilderView({
        namespace: ModuleInterpreterWorker,
        icon: InterpreterBase.svgIcon
    })
    export class Module extends InterpreterBase.Module {

        constructor( params ){
            super(
                params,
                ({data, configuration, context}) => this.interpret(data, configuration, context),
                transferabilityExpectation )
            
        }

        interpret( data: any, configuration: PersistentData, context: Context ) {
           
            let workerPool = this.environment.workerPool
            let channel$ = workerPool.schedule<WorkerArguments>({
                title: 'Interpret',
                entryPoint: interpretInWorker,
                args:{ 
                    script:configuration.code,
                    packages: configuration.getPackages(),
                    data: data,
                    outputsCount: this.outputs$.length,
                    dataframePaths: findDataframePaths(window, data)
                },
                context
            })
            
            channel$.pipe( 
                filter( ({type}) => type == "Exit")
            ).subscribe( 
                () => {
                    context.terminate()
                },
                (error) => { 
                    context.error(new ModuleError(this, error.message))
                    context.terminate()
                }
            )
            channel$.pipe(
                filter( ({type}) => type == "Data")
            ).subscribe( 
                ({data}) => {
                    let formatted = recoverDataFrames(window, data.data, data.dataframePaths)
                    this.outputs$[data.outputIndex].next( {data: formatted, context: context})
                }
            )
        }
    }
}