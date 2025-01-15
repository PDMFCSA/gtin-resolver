class Lock {    
    constructor() {        
        this.queue = [];        
        this.locked = false;
    }       
    async acquire() {        
        const self = this;        
        if (self.locked) {            
            return new Promise(((resolve) => self.queue.push(resolve)));
        }        
        else {            
            self.locked = true;           
            return Promise.resolve();        
        }}        
    
    release() {        
        const self = this;        
        const next = self.queue.shift();        
        if(next) {            
            const cb = () => {
                next();
            };            
            if (typeof globalThis.window === 'undefined')                
                globalThis.process.nextTick(cb);            
            else                
                setTimeout(cb, 0);        
        }else {          
            self.locked = false;        
        } 
    }
}

module.exports = Lock;