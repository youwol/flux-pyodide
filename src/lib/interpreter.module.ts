import { getUrlBase } from '@youwol/cdn-client'
import { Context, expectAnyOf, expectAttribute, expect as expect_, expectCount,
    BuilderView, Flux, Property, RenderView, Schema, ModuleFlux, Pipe, freeContract, ModuleError
} from '@youwol/flux-core'
import { attr$, render } from "@youwol/flux-view"

import{pack, pyodide} from './main'
import{DataFrame, Serie} from '@youwol/dataframe'


/**
  ## Presentation


 ## Resources
 [Pyodide 0.17](https://hacks.mozilla.org/2021/04/pyodide-spin-out-and-0-17-release/)
 [Pyodide FAQ](https://pyodide.org/en/latest/usage/faq.html)

 */
export namespace ModuleInterpreter{

    //Icons made by <a href="https://www.flaticon.com/authors/freepik" title="Freepik">Freepik</a> from <a href="https://www.flaticon.com/" title="Flaticon"> www.flaticon.com</a>
    let svgIcon = `
    <g>
        <g>
            <path style="fill:#030104;" d="M11.298,8.02c1.295-0.587,1.488-5.055,0.724-6.371c-0.998-1.718-5.742-1.373-7.24-0.145    C4.61,2.114,4.628,3.221,4.636,4.101h4.702v0.412H4.637c0,0.006-2.093,0.013-2.093,0.013c-3.609,0-3.534,7.838,1.228,7.838    c0,0,0.175-1.736,0.481-2.606C5.198,7.073,9.168,8.986,11.298,8.02z M6.375,3.465c-0.542,0-0.981-0.439-0.981-0.982    c0-0.542,0.439-0.982,0.981-0.982c0.543,0,0.982,0.44,0.982,0.982C7.358,3.025,6.918,3.465,6.375,3.465z"/>
            <path style="fill:#030104;" d="M13.12,4.691c0,0-0.125,1.737-0.431,2.606c-0.945,2.684-4.914,0.772-7.045,1.738    C4.35,9.622,4.155,14.09,4.92,15.406c0.997,1.719,5.741,1.374,7.24,0.145c0.172-0.609,0.154-1.716,0.146-2.596H7.603v-0.412h4.701    c0-0.006,2.317-0.013,2.317-0.013C17.947,12.53,18.245,4.691,13.12,4.691z M10.398,13.42c0.542,0,0.982,0.439,0.982,0.982    c0,0.542-0.44,0.981-0.982,0.981s-0.981-0.439-0.981-0.981C9.417,13.859,9.856,13.42,10.398,13.42z"/>
        </g>
    </g>`

    let defaultCode = `
import sys
def processing(data, context, outputs):
    context.info("Hello python interpreter", data)
    outputs[0].next({"data":data, "context": context})

processing
`
    let defaultPackages = `
// e.g. ['numpy', 'pandas']
// the list of available packages can be found here:  ${pack.urlCDN}/dist/docs/index.html
return []
    `

    /**
     * ## Persistent Data  🔧
     *
     */
    @Schema({
        pack
    })
    export class PersistentData {

        @Property({
            description: "Code to interpret",
            type: "code"
        })
        readonly code: string = defaultCode

        @Property({
            description: "packages to load in the interpreter",
            type: 'code'
        })
        readonly packages: string = defaultPackages

        @Property({
            description: "Number of outputs",
            type: 'integer'
        })
        readonly outputCount: number = 1


        getPackages(){
            if(Array.isArray(this.packages))
                return this.packages
            return new Function(this.packages)()
        }

        constructor({code,packages, outputCount} :{
            code?:string,
            packages?: string,
            outputCount?: number
        } = {}) {
            
            const filtered = Object.entries({code, packages, outputCount})
            .filter( ([k,v]) => v != undefined)
            .reduce((acc, [k,v]) => ({...acc, ...{[k]: v}}), {});

            Object.assign(this, filtered)
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
        icon: svgIcon
    })
    export class Module extends ModuleFlux {

        outputs$ = new Array<any>()

        constructor( params ){
            super(params)
            let conf = this.getPersistentData<PersistentData>()
            this.addInput({
                id:'input',
                description: 'trigger script execution',
                contract: freeContract(),
                onTriggered: ({data, configuration, context}) => this.interpret(data, configuration, context)
            })
            for(let i=0; i<conf.outputCount; i++){
                let output$ = this.addOutput({id:`output_${i}`})

                this.outputs$.push( 
                    {
                        'next' : (pythonArg) => { 
                            let s = Object.fromEntries(pythonArg.toJs().entries());
                            console.log("blablbalbal", s)
                            output$.next(s as any)
                        } 
                    }
                )
            }
            
        }

        interpret( data: any, configuration: PersistentData, context: Context ) {


            Promise.all(configuration.getPackages().map( pack => pyodide.loadPackage(pack)))
            .then( () => {
                context.withChild("script execution", (ctx) => {
                    try{
                        ctx.info("Dependencies loaded successfully", {packages: configuration.getPackages()})

                        let fct = pyodide.runPython(configuration.code)
                        let result = fct(data, context, this.outputs$)
                        //ctx.info("Python run successfully", {result})
                        //this.result$.next({data:result, context})
                        ctx.terminate() 
                    }
                    catch(e){
                        throw new ModuleError(this, e)
                    }
                })
            })
        }
    }
}