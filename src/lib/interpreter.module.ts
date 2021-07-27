import { getUrlBase } from '@youwol/cdn-client'
import { Context, expectAnyOf, expectAttribute, expect as expect_, expectCount,
    BuilderView, Flux, Property, RenderView, Schema, ModuleFlux, Pipe, freeContract, ModuleError
} from '@youwol/flux-core'
import { attr$, render } from "@youwol/flux-view"

import{pack, pyodide} from './main'
import{DataFrame, Serie} from '@youwol/dataframe'
import { InterpreterBase } from './interpreter-base.module'
import { of, Subject } from 'rxjs'
import { outputPython2Js } from './utilities'


/**
  ## Presentation


 ## Resources
 [Pyodide 0.17](https://hacks.mozilla.org/2021/04/pyodide-spin-out-and-0-17-release/)
 [Pyodide FAQ](https://pyodide.org/en/latest/usage/faq.html)

 */
export namespace ModuleInterpreter{

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

    /** ## Module
     **/
    @Flux({
        pack: pack,
        namespace: ModuleInterpreter,
        id: "ModuleInterpreter",
        displayName: "Python",
        description: "Python interpreter",
        resources: {
            'technical doc': `${pack.urlCDN}/dist/docs/modules/`
        }
    })
    @BuilderView({
        namespace: ModuleInterpreter,
        icon: InterpreterBase.svgIcon
    })
    export class Module extends InterpreterBase.Module {

        static loadedPackages = []

        constructor( params ){
            super(
                params, 
                ({data, configuration, context}) => this.interpret(data, configuration, context),
                freeContract() 
                )            
        }

        interpret( data: any, configuration: PersistentData, context: Context ) {
           
            Promise.all(
                configuration.getPackages()
                .filter( pack => !Module.loadedPackages.includes(pack))
                .map( pack => {
                    let p = this.environment.exposeProcess({
                        title:`Python interpreter: Load package ${pack} in master worker`
                    })
                    p.start()
                    // that would be great to have access to the request's loading progress
                    return pyodide.loadPackage(pack).then( () => {
                        Module.loadedPackages.push(pack)
                        p.succeed()
                    })                            
            }))
            .then( () => {
                context.withChild("script execution", (ctx) => {
                    try{
                        ctx.info("Dependencies loaded successfully", {packages: configuration.getPackages()})
                        let outputsInScript$ = this.outputs$.map( (output$) => {
                            return {
                                'next' : (fromScript) => { 
                                    let s = outputPython2Js(fromScript);
                                    output$.next( {data: s, context} )
                                } 
                            } 
                        })
                        let fct = pyodide.runPython(configuration.code)
                        fct(data, context, outputsInScript$)
                        ctx.terminate() 
                    }
                    catch(e){
                        ctx.error(e)
                        throw new ModuleError(this, e)
                    }
                })
            })
        }
    }
}