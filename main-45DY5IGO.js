var yy=Object.defineProperty,vy=Object.defineProperties;var by=Object.getOwnPropertyDescriptors;var vu=Object.getOwnPropertySymbols;var _y=Object.prototype.hasOwnProperty,Dy=Object.prototype.propertyIsEnumerable;var bu=(e,n,t)=>n in e?yy(e,n,{enumerable:!0,configurable:!0,writable:!0,value:t}):e[n]=t,q=(e,n)=>{for(var t in n||={})_y.call(n,t)&&bu(e,t,n[t]);if(vu)for(var t of vu(n))Dy.call(n,t)&&bu(e,t,n[t]);return e},Je=(e,n)=>vy(e,by(n));var _e=null,qi=!1,_a=1,wy=null,ft=Symbol("SIGNAL");function M(e){let n=_e;return _e=e,n}function Zi(){return _e}var Qi={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function _u(e){if(qi)throw new Error("");if(_e===null)return;_e.consumerOnSignalRead(e);let n=_e.producersTail;if(n!==void 0&&n.producer===e)return;let t,r=_e.recomputing;if(r&&(t=n!==void 0?n.nextProducer:_e.producers,t!==void 0&&t.producer===e)){_e.producersTail=t,t.lastReadVersion=e.version;return}let i=e.consumersTail;if(i!==void 0&&i.consumer===_e&&(!r||Ey(i,_e)))return;let o=Zn(_e),s={producer:e,consumer:_e,nextProducer:t,prevConsumer:i,lastReadVersion:e.version,nextConsumer:void 0};_e.producersTail=s,n!==void 0?n.nextProducer=s:_e.producers=s,o&&Tu(e,s)}function Du(){_a++}function wu(e){if(!(Zn(e)&&!e.dirty)&&!(!e.dirty&&e.lastCleanEpoch===_a)){if(!e.producerMustRecompute(e)&&!Ea(e)){ba(e);return}e.producerRecomputeValue(e),ba(e)}}function Da(e){if(e.consumers===void 0)return;let n=qi;qi=!0;try{for(let t=e.consumers;t!==void 0;t=t.nextConsumer){let r=t.consumer;r.dirty||Cy(r)}}finally{qi=n}}function wa(){return _e?.consumerAllowSignalWrites!==!1}function Cy(e){e.dirty=!0,Da(e),e.consumerMarkedDirty?.(e)}function ba(e){e.dirty=!1,e.lastCleanEpoch=_a}function Ca(e){return e&&Cu(e),M(e)}function Cu(e){e.producersTail=void 0,e.recomputing=!0}function Eu(e,n){M(n),e&&Iu(e)}function Iu(e){e.recomputing=!1;let n=e.producersTail,t=n!==void 0?n.nextProducer:e.producers;if(t!==void 0){if(Zn(e))do t=Ia(t);while(t!==void 0);n!==void 0?n.nextProducer=void 0:e.producers=void 0}}function Ea(e){for(let n=e.producers;n!==void 0;n=n.nextProducer){let t=n.producer,r=n.lastReadVersion;if(r!==t.version||(wu(t),r!==t.version))return!0}return!1}function Yi(e){if(Zn(e)){let n=e.producers;for(;n!==void 0;)n=Ia(n)}e.producers=void 0,e.producersTail=void 0,e.consumers=void 0,e.consumersTail=void 0}function Tu(e,n){let t=e.consumersTail,r=Zn(e);if(t!==void 0?(n.nextConsumer=t.nextConsumer,t.nextConsumer=n):(n.nextConsumer=void 0,e.consumers=n),n.prevConsumer=t,e.consumersTail=n,!r)for(let i=e.producers;i!==void 0;i=i.nextProducer)Tu(i.producer,i)}function Ia(e){let n=e.producer,t=e.nextProducer,r=e.nextConsumer,i=e.prevConsumer;if(e.nextConsumer=void 0,e.prevConsumer=void 0,r!==void 0?r.prevConsumer=i:n.consumersTail=i,i!==void 0)i.nextConsumer=r;else if(n.consumers=r,!Zn(n)){let o=n.producers;for(;o!==void 0;)o=Ia(o)}return t}function Zn(e){return e.consumerIsAlwaysLive||e.consumers!==void 0}function Mu(e){wy?.(e)}function Ey(e,n){let t=n.producersTail;if(t!==void 0){let r=n.producers;do{if(r===e)return!0;if(r===t)break;r=r.nextProducer}while(r!==void 0)}return!1}function Su(e,n){return Object.is(e,n)}function Iy(){throw new Error}var xu=Iy;function Ru(e){xu(e)}function Ta(e){xu=e}var Ty=null;function Ma(e,n){let t=Object.create(ku);t.value=e,n!==void 0&&(t.equal=n);let r=()=>Au(t);return r[ft]=t,Mu(t),[r,s=>Sa(t,s),s=>Nu(t,s)]}function Au(e){return _u(e),e.value}function Sa(e,n){wa()||Ru(e),e.equal(e.value,n)||(e.value=n,My(e))}function Nu(e,n){wa()||Ru(e),Sa(e,n(e.value))}var ku=Je(q({},Qi),{equal:Su,value:void 0,kind:"signal"});function My(e){e.version++,Du(),Da(e),Ty?.(e)}function S(e){return typeof e=="function"}function Ki(e){let t=e(r=>{Error.call(r),r.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Xi=Ki(e=>function(t){e(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((r,i)=>`${i+1}) ${r.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function vn(e,n){if(e){let t=e.indexOf(n);0<=t&&e.splice(t,1)}}var X=class e{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let o of t)o.remove(this);else t.remove(this);let{initialTeardown:r}=this;if(S(r))try{r()}catch(o){n=o instanceof Xi?o.errors:[o]}let{_finalizers:i}=this;if(i){this._finalizers=null;for(let o of i)try{Ou(o)}catch(s){n=n??[],s instanceof Xi?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Xi(n)}}add(n){var t;if(n&&n!==this)if(this.closed)Ou(n);else{if(n instanceof e){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(n)}}_hasParent(n){let{_parentage:t}=this;return t===n||Array.isArray(t)&&t.includes(n)}_addParent(n){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(n),t):t?[t,n]:n}_removeParent(n){let{_parentage:t}=this;t===n?this._parentage=null:Array.isArray(t)&&vn(t,n)}remove(n){let{_finalizers:t}=this;t&&vn(t,n),n instanceof e&&n._removeParent(this)}};X.EMPTY=(()=>{let e=new X;return e.closed=!0,e})();var xa=X.EMPTY;function Ji(e){return e instanceof X||e&&"closed"in e&&S(e.remove)&&S(e.add)&&S(e.unsubscribe)}function Ou(e){S(e)?e():e.unsubscribe()}var et={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Qn={setTimeout(e,n,...t){let{delegate:r}=Qn;return r?.setTimeout?r.setTimeout(e,n,...t):setTimeout(e,n,...t)},clearTimeout(e){let{delegate:n}=Qn;return(n?.clearTimeout||clearTimeout)(e)},delegate:void 0};function eo(e){Qn.setTimeout(()=>{let{onUnhandledError:n}=et;if(n)n(e);else throw e})}function Ur(){}var Fu=Ra("C",void 0,void 0);function Pu(e){return Ra("E",void 0,e)}function Lu(e){return Ra("N",e,void 0)}function Ra(e,n,t){return{kind:e,value:n,error:t}}var bn=null;function Yn(e){if(et.useDeprecatedSynchronousErrorHandling){let n=!bn;if(n&&(bn={errorThrown:!1,error:null}),e(),n){let{errorThrown:t,error:r}=bn;if(bn=null,t)throw r}}else e()}function ju(e){et.useDeprecatedSynchronousErrorHandling&&bn&&(bn.errorThrown=!0,bn.error=e)}var _n=class extends X{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Ji(n)&&n.add(this)):this.destination=Ry}static create(n,t,r){return new Kn(n,t,r)}next(n){this.isStopped?Na(Lu(n),this):this._next(n)}error(n){this.isStopped?Na(Pu(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Na(Fu,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},Sy=Function.prototype.bind;function Aa(e,n){return Sy.call(e,n)}var ka=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:t}=this;if(t.next)try{t.next(n)}catch(r){to(r)}}error(n){let{partialObserver:t}=this;if(t.error)try{t.error(n)}catch(r){to(r)}else to(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(t){to(t)}}},Kn=class extends _n{constructor(n,t,r){super();let i;if(S(n)||!n)i={next:n??void 0,error:t??void 0,complete:r??void 0};else{let o;this&&et.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),i={next:n.next&&Aa(n.next,o),error:n.error&&Aa(n.error,o),complete:n.complete&&Aa(n.complete,o)}):i=n}this.destination=new ka(i)}};function to(e){et.useDeprecatedSynchronousErrorHandling?ju(e):eo(e)}function xy(e){throw e}function Na(e,n){let{onStoppedNotification:t}=et;t&&Qn.setTimeout(()=>t(e,n))}var Ry={closed:!0,next:Ur,error:xy,complete:Ur};var Xn=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Rt(e){return e}function Bu(e){return e.length===0?Rt:e.length===1?e[0]:function(t){return e.reduce((r,i)=>i(r),t)}}var N=(()=>{class e{constructor(t){t&&(this._subscribe=t)}lift(t){let r=new e;return r.source=this,r.operator=t,r}subscribe(t,r,i){let o=Ny(t)?t:new Kn(t,r,i);return Yn(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(t){try{return this._subscribe(t)}catch(r){t.error(r)}}forEach(t,r){return r=Vu(r),new r((i,o)=>{let s=new Kn({next:a=>{try{t(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:i});this.subscribe(s)})}_subscribe(t){var r;return(r=this.source)===null||r===void 0?void 0:r.subscribe(t)}[Xn](){return this}pipe(...t){return Bu(t)(this)}toPromise(t){return t=Vu(t),new t((r,i)=>{let o;this.subscribe(s=>o=s,s=>i(s),()=>r(o))})}}return e.create=n=>new e(n),e})();function Vu(e){var n;return(n=e??et.Promise)!==null&&n!==void 0?n:Promise}function Ay(e){return e&&S(e.next)&&S(e.error)&&S(e.complete)}function Ny(e){return e&&e instanceof _n||Ay(e)&&Ji(e)}function Oa(e){return S(e?.lift)}function ie(e){return n=>{if(Oa(n))return n.lift(function(t){try{return e(t,this)}catch(r){this.error(r)}});throw new TypeError("Unable to lift unknown Observable type")}}function z(e,n,t,r,i){return new Fa(e,n,t,r,i)}var Fa=class extends _n{constructor(n,t,r,i,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=t?function(a){try{t(a)}catch(c){n.error(c)}}:super._next,this._error=i?function(a){try{i(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=r?function(){try{r()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};function Hu(){return ie((e,n)=>{let t=null;e._refCount++;let r=z(n,void 0,void 0,void 0,()=>{if(!e||e._refCount<=0||0<--e._refCount){t=null;return}let i=e._connection,o=t;t=null,i&&(!o||i===o)&&i.unsubscribe(),n.unsubscribe()});e.subscribe(r),r.closed||(t=e.connect())})}var $r=class extends N{constructor(n,t){super(),this.source=n,this.subjectFactory=t,this._subject=null,this._refCount=0,this._connection=null,Oa(n)&&(this.lift=n.lift)}_subscribe(n){return this.getSubject().subscribe(n)}getSubject(){let n=this._subject;return(!n||n.isStopped)&&(this._subject=this.subjectFactory()),this._subject}_teardown(){this._refCount=0;let{_connection:n}=this;this._subject=this._connection=null,n?.unsubscribe()}connect(){let n=this._connection;if(!n){n=this._connection=new X;let t=this.getSubject();n.add(this.source.subscribe(z(t,void 0,()=>{this._teardown(),t.complete()},r=>{this._teardown(),t.error(r)},()=>this._teardown()))),n.closed&&(this._connection=null,n=X.EMPTY)}return n}refCount(){return Hu()(this)}};var Jn={schedule(e){let n=requestAnimationFrame,t=cancelAnimationFrame,{delegate:r}=Jn;r&&(n=r.requestAnimationFrame,t=r.cancelAnimationFrame);let i=n(o=>{t=void 0,e(o)});return new X(()=>t?.(i))},requestAnimationFrame(...e){let{delegate:n}=Jn;return(n?.requestAnimationFrame||requestAnimationFrame)(...e)},cancelAnimationFrame(...e){let{delegate:n}=Jn;return(n?.cancelAnimationFrame||cancelAnimationFrame)(...e)},delegate:void 0};var Uu=Ki(e=>function(){e(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var J=(()=>{class e extends N{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let r=new no(this,this);return r.operator=t,r}_throwIfClosed(){if(this.closed)throw new Uu}next(t){Yn(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let r of this.currentObservers)r.next(t)}})}error(t){Yn(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:r}=this;for(;r.length;)r.shift().error(t)}})}complete(){Yn(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:r,isStopped:i,observers:o}=this;return r||i?xa:(this.currentObservers=null,o.push(t),new X(()=>{this.currentObservers=null,vn(o,t)}))}_checkFinalizedStatuses(t){let{hasError:r,thrownError:i,isStopped:o}=this;r?t.error(i):o&&t.complete()}asObservable(){let t=new N;return t.source=this,t}}return e.create=(n,t)=>new no(n,t),e})(),no=class extends J{constructor(n,t){super(),this.destination=n,this.source=t}next(n){var t,r;(r=(t=this.destination)===null||t===void 0?void 0:t.next)===null||r===void 0||r.call(t,n)}error(n){var t,r;(r=(t=this.destination)===null||t===void 0?void 0:t.error)===null||r===void 0||r.call(t,n)}complete(){var n,t;(t=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||t===void 0||t.call(n)}_subscribe(n){var t,r;return(r=(t=this.source)===null||t===void 0?void 0:t.subscribe(n))!==null&&r!==void 0?r:xa}};var At=class extends J{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let t=super._subscribe(n);return!t.closed&&n.next(this._value),t}getValue(){let{hasError:n,thrownError:t,_value:r}=this;if(n)throw t;return this._throwIfClosed(),r}next(n){super.next(this._value=n)}};var Pa={now(){return(Pa.delegate||Date).now()},delegate:void 0};var ro=class extends X{constructor(n,t){super()}schedule(n,t=0){return this}};var zr={setInterval(e,n,...t){let{delegate:r}=zr;return r?.setInterval?r.setInterval(e,n,...t):setInterval(e,n,...t)},clearInterval(e){let{delegate:n}=zr;return(n?.clearInterval||clearInterval)(e)},delegate:void 0};var Yt=class extends ro{constructor(n,t){super(n,t),this.scheduler=n,this.work=t,this.pending=!1}schedule(n,t=0){var r;if(this.closed)return this;this.state=n;let i=this.id,o=this.scheduler;return i!=null&&(this.id=this.recycleAsyncId(o,i,t)),this.pending=!0,this.delay=t,this.id=(r=this.id)!==null&&r!==void 0?r:this.requestAsyncId(o,this.id,t),this}requestAsyncId(n,t,r=0){return zr.setInterval(n.flush.bind(n,this),r)}recycleAsyncId(n,t,r=0){if(r!=null&&this.delay===r&&this.pending===!1)return t;t!=null&&zr.clearInterval(t)}execute(n,t){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let r=this._execute(n,t);if(r)return r;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,t){let r=!1,i;try{this.work(n)}catch(o){r=!0,i=o||new Error("Scheduled action threw falsy error")}if(r)return this.unsubscribe(),i}unsubscribe(){if(!this.closed){let{id:n,scheduler:t}=this,{actions:r}=t;this.work=this.state=this.scheduler=null,this.pending=!1,vn(r,this),n!=null&&(this.id=this.recycleAsyncId(t,n,null)),this.delay=null,super.unsubscribe()}}};var ky=1,La,ja={};function $u(e){return e in ja?(delete ja[e],!0):!1}var zu={setImmediate(e){let n=ky++;return ja[n]=!0,La||(La=Promise.resolve()),La.then(()=>$u(n)&&e()),n},clearImmediate(e){$u(e)}};var{setImmediate:Oy,clearImmediate:Fy}=zu,Gr={setImmediate(...e){let{delegate:n}=Gr;return(n?.setImmediate||Oy)(...e)},clearImmediate(e){let{delegate:n}=Gr;return(n?.clearImmediate||Fy)(e)},delegate:void 0};var io=class extends Yt{constructor(n,t){super(n,t),this.scheduler=n,this.work=t}requestAsyncId(n,t,r=0){return r!==null&&r>0?super.requestAsyncId(n,t,r):(n.actions.push(this),n._scheduled||(n._scheduled=Gr.setImmediate(n.flush.bind(n,void 0))))}recycleAsyncId(n,t,r=0){var i;if(r!=null?r>0:this.delay>0)return super.recycleAsyncId(n,t,r);let{actions:o}=n;t!=null&&((i=o[o.length-1])===null||i===void 0?void 0:i.id)!==t&&(Gr.clearImmediate(t),n._scheduled===t&&(n._scheduled=void 0))}};var er=class e{constructor(n,t=e.now){this.schedulerActionCtor=n,this.now=t}schedule(n,t=0,r){return new this.schedulerActionCtor(this,n).schedule(r,t)}};er.now=Pa.now;var Kt=class extends er{constructor(n,t=er.now){super(n,t),this.actions=[],this._active=!1}flush(n){let{actions:t}=this;if(this._active){t.push(n);return}let r;this._active=!0;do if(r=n.execute(n.state,n.delay))break;while(n=t.shift());if(this._active=!1,r){for(;n=t.shift();)n.unsubscribe();throw r}}};var oo=class extends Kt{flush(n){this._active=!0;let t=this._scheduled;this._scheduled=void 0;let{actions:r}=this,i;n=n||r.shift();do if(i=n.execute(n.state,n.delay))break;while((n=r[0])&&n.id===t&&r.shift());if(this._active=!1,i){for(;(n=r[0])&&n.id===t&&r.shift();)n.unsubscribe();throw i}}};var so=new oo(io);var Ba=new Kt(Yt),Gu=Ba;var ao=class extends Yt{constructor(n,t){super(n,t),this.scheduler=n,this.work=t}requestAsyncId(n,t,r=0){return r!==null&&r>0?super.requestAsyncId(n,t,r):(n.actions.push(this),n._scheduled||(n._scheduled=Jn.requestAnimationFrame(()=>n.flush(void 0))))}recycleAsyncId(n,t,r=0){var i;if(r!=null?r>0:this.delay>0)return super.recycleAsyncId(n,t,r);let{actions:o}=n;t!=null&&t===n._scheduled&&((i=o[o.length-1])===null||i===void 0?void 0:i.id)!==t&&(Jn.cancelAnimationFrame(t),n._scheduled=void 0)}};var co=class extends Kt{flush(n){this._active=!0;let t;n?t=n.id:(t=this._scheduled,this._scheduled=void 0);let{actions:r}=this,i;n=n||r.shift();do if(i=n.execute(n.state,n.delay))break;while((n=r[0])&&n.id===t&&r.shift());if(this._active=!1,i){for(;(n=r[0])&&n.id===t&&r.shift();)n.unsubscribe();throw i}}};var lo=new co(ao);var Wu=new N(e=>e.complete());function uo(e){return e&&S(e.schedule)}function Va(e){return e[e.length-1]}function qu(e){return S(Va(e))?e.pop():void 0}function tr(e){return uo(Va(e))?e.pop():void 0}function Zu(e,n){return typeof Va(e)=="number"?e.pop():n}function Yu(e,n,t,r){function i(o){return o instanceof t?o:new t(function(s){s(o)})}return new(t||(t=Promise))(function(o,s){function a(d){try{l(r.next(d))}catch(u){s(u)}}function c(d){try{l(r.throw(d))}catch(u){s(u)}}function l(d){d.done?o(d.value):i(d.value).then(a,c)}l((r=r.apply(e,n||[])).next())})}function Qu(e){var n=typeof Symbol=="function"&&Symbol.iterator,t=n&&e[n],r=0;if(t)return t.call(e);if(e&&typeof e.length=="number")return{next:function(){return e&&r>=e.length&&(e=void 0),{value:e&&e[r++],done:!e}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Dn(e){return this instanceof Dn?(this.v=e,this):new Dn(e)}function Ku(e,n,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var r=t.apply(e,n||[]),i,o=[];return i=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),i[Symbol.asyncIterator]=function(){return this},i;function s(p){return function(m){return Promise.resolve(m).then(p,u)}}function a(p,m){r[p]&&(i[p]=function(D){return new Promise(function(y,v){o.push([p,D,y,v])>1||c(p,D)})},m&&(i[p]=m(i[p])))}function c(p,m){try{l(r[p](m))}catch(D){h(o[0][3],D)}}function l(p){p.value instanceof Dn?Promise.resolve(p.value.v).then(d,u):h(o[0][2],p)}function d(p){c("next",p)}function u(p){c("throw",p)}function h(p,m){p(m),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Xu(e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=e[Symbol.asyncIterator],t;return n?n.call(e):(e=typeof Qu=="function"?Qu(e):e[Symbol.iterator](),t={},r("next"),r("throw"),r("return"),t[Symbol.asyncIterator]=function(){return this},t);function r(o){t[o]=e[o]&&function(s){return new Promise(function(a,c){s=e[o](s),i(a,c,s.done,s.value)})}}function i(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var fo=e=>e&&typeof e.length=="number"&&typeof e!="function";function po(e){return S(e?.then)}function ho(e){return S(e[Xn])}function mo(e){return Symbol.asyncIterator&&S(e?.[Symbol.asyncIterator])}function go(e){return new TypeError(`You provided ${e!==null&&typeof e=="object"?"an invalid object":`'${e}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function Py(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var yo=Py();function vo(e){return S(e?.[yo])}function bo(e){return Ku(this,arguments,function*(){let t=e.getReader();try{for(;;){let{value:r,done:i}=yield Dn(t.read());if(i)return yield Dn(void 0);yield yield Dn(r)}}finally{t.releaseLock()}})}function _o(e){return S(e?.getReader)}function fe(e){if(e instanceof N)return e;if(e!=null){if(ho(e))return Ly(e);if(fo(e))return jy(e);if(po(e))return By(e);if(mo(e))return Ju(e);if(vo(e))return Vy(e);if(_o(e))return Hy(e)}throw go(e)}function Ly(e){return new N(n=>{let t=e[Xn]();if(S(t.subscribe))return t.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function jy(e){return new N(n=>{for(let t=0;t<e.length&&!n.closed;t++)n.next(e[t]);n.complete()})}function By(e){return new N(n=>{e.then(t=>{n.closed||(n.next(t),n.complete())},t=>n.error(t)).then(null,eo)})}function Vy(e){return new N(n=>{for(let t of e)if(n.next(t),n.closed)return;n.complete()})}function Ju(e){return new N(n=>{Uy(e,n).catch(t=>n.error(t))})}function Hy(e){return Ju(bo(e))}function Uy(e,n){var t,r,i,o;return Yu(this,void 0,void 0,function*(){try{for(t=Xu(e);r=yield t.next(),!r.done;){let s=r.value;if(n.next(s),n.closed)return}}catch(s){i={error:s}}finally{try{r&&!r.done&&(o=t.return)&&(yield o.call(t))}finally{if(i)throw i.error}}n.complete()})}function Ne(e,n,t,r=0,i=!1){let o=n.schedule(function(){t(),i?e.add(this.schedule(null,r)):this.unsubscribe()},r);if(e.add(o),!i)return o}function Do(e,n=0){return ie((t,r)=>{t.subscribe(z(r,i=>Ne(r,e,()=>r.next(i),n),()=>Ne(r,e,()=>r.complete(),n),i=>Ne(r,e,()=>r.error(i),n)))})}function wo(e,n=0){return ie((t,r)=>{r.add(e.schedule(()=>t.subscribe(r),n))})}function ef(e,n){return fe(e).pipe(wo(n),Do(n))}function tf(e,n){return fe(e).pipe(wo(n),Do(n))}function nf(e,n){return new N(t=>{let r=0;return n.schedule(function(){r===e.length?t.complete():(t.next(e[r++]),t.closed||this.schedule())})})}function rf(e,n){return new N(t=>{let r;return Ne(t,n,()=>{r=e[yo](),Ne(t,n,()=>{let i,o;try{({value:i,done:o}=r.next())}catch(s){t.error(s);return}o?t.complete():t.next(i)},0,!0)}),()=>S(r?.return)&&r.return()})}function Co(e,n){if(!e)throw new Error("Iterable cannot be null");return new N(t=>{Ne(t,n,()=>{let r=e[Symbol.asyncIterator]();Ne(t,n,()=>{r.next().then(i=>{i.done?t.complete():t.next(i.value)})},0,!0)})})}function of(e,n){return Co(bo(e),n)}function sf(e,n){if(e!=null){if(ho(e))return ef(e,n);if(fo(e))return nf(e,n);if(po(e))return tf(e,n);if(mo(e))return Co(e,n);if(vo(e))return rf(e,n);if(_o(e))return of(e,n)}throw go(e)}function wn(e,n){return n?sf(e,n):fe(e)}function pt(...e){let n=tr(e);return wn(e,n)}function Eo(e){return!!e&&(e instanceof N||S(e.lift)&&S(e.subscribe))}function af(e){return e instanceof Date&&!isNaN(e)}function ze(e,n){return ie((t,r)=>{let i=0;t.subscribe(z(r,o=>{r.next(e.call(n,o,i++))}))})}var{isArray:$y}=Array;function zy(e,n){return $y(n)?e(...n):e(n)}function cf(e){return ze(n=>zy(e,n))}var{isArray:Gy}=Array,{getPrototypeOf:Wy,prototype:qy,keys:Zy}=Object;function lf(e){if(e.length===1){let n=e[0];if(Gy(n))return{args:n,keys:null};if(Qy(n)){let t=Zy(n);return{args:t.map(r=>n[r]),keys:t}}}return{args:e,keys:null}}function Qy(e){return e&&typeof e=="object"&&Wy(e)===qy}function df(e,n){return e.reduce((t,r,i)=>(t[r]=n[i],t),{})}function Wr(...e){let n=tr(e),t=qu(e),{args:r,keys:i}=lf(e);if(r.length===0)return wn([],n);let o=new N(Yy(r,n,i?s=>df(i,s):Rt));return t?o.pipe(cf(t)):o}function Yy(e,n,t=Rt){return r=>{uf(n,()=>{let{length:i}=e,o=new Array(i),s=i,a=i;for(let c=0;c<i;c++)uf(n,()=>{let l=wn(e[c],n),d=!1;l.subscribe(z(r,u=>{o[c]=u,d||(d=!0,a--),a||r.next(t(o.slice()))},()=>{--s||r.complete()}))},r)},r)}}function uf(e,n,t){e?Ne(t,e,n):n()}function ff(e,n,t,r,i,o,s,a){let c=[],l=0,d=0,u=!1,h=()=>{u&&!c.length&&!l&&n.complete()},p=D=>l<r?m(D):c.push(D),m=D=>{o&&n.next(D),l++;let y=!1;fe(t(D,d++)).subscribe(z(n,v=>{i?.(v),o?p(v):n.next(v)},()=>{y=!0},void 0,()=>{if(y)try{for(l--;c.length&&l<r;){let v=c.shift();s?Ne(n,s,()=>m(v)):m(v)}h()}catch(v){n.error(v)}}))};return e.subscribe(z(n,p,()=>{u=!0,h()})),()=>{a?.()}}function nr(e,n,t=1/0){return S(n)?nr((r,i)=>ze((o,s)=>n(r,o,i,s))(fe(e(r,i))),t):(typeof n=="number"&&(t=n),ie((r,i)=>ff(r,i,e,t)))}function pf(e=1/0){return nr(Rt,e)}function hf(e=0,n,t=Gu){let r=-1;return n!=null&&(uo(n)?t=n:r=n),new N(i=>{let o=af(e)?+e-t.now():e;o<0&&(o=0);let s=0;return t.schedule(function(){i.closed||(i.next(s++),0<=r?this.schedule(void 0,r):i.complete())},o)})}function Ha(...e){let n=tr(e),t=Zu(e,1/0),r=e;return r.length?r.length===1?fe(r[0]):pf(t)(wn(r,n)):Wu}function rr(e,n){return ie((t,r)=>{let i=0;t.subscribe(z(r,o=>e.call(n,o,i++)&&r.next(o)))})}function mf(e){return ie((n,t)=>{let r=!1,i=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,r){r=!1;let l=i;i=null,t.next(l)}s&&t.complete()},c=()=>{o=null,s&&t.complete()};n.subscribe(z(t,l=>{r=!0,i=l,o||fe(e(l)).subscribe(o=z(t,a,c))},()=>{s=!0,(!r||!o||o.closed)&&t.complete()}))})}function qr(e,n=Ba){return mf(()=>hf(e,n))}function Ua(e,n){return S(n)?nr(e,n,1):nr(e,1)}function Io(e,n=Rt){return e=e??Ky,ie((t,r)=>{let i,o=!0;t.subscribe(z(r,s=>{let a=n(s);(o||!e(i,a))&&(o=!1,i=a,r.next(s))}))})}function Ky(e,n){return e===n}function $a(e){return ie((n,t)=>{try{n.subscribe(t)}finally{t.add(e)}})}function za(e){return rr((n,t)=>e<=t)}function To(e,n){return ie((t,r)=>{let i=null,o=0,s=!1,a=()=>s&&!i&&r.complete();t.subscribe(z(r,c=>{i?.unsubscribe();let l=0,d=o++;fe(e(c,d)).subscribe(i=z(r,u=>r.next(n?n(c,u,d,l++):u),()=>{i=null,a()}))},()=>{s=!0,a()}))})}function tt(e){return ie((n,t)=>{fe(e).subscribe(z(t,()=>t.complete(),Ur)),!t.closed&&n.subscribe(t)})}var Ga;function Mo(){return Ga}function ht(e){let n=Ga;return Ga=e,n}var gf=Symbol("NotFound");function ir(e){return e===gf||e?.name==="\u0275NotFound"}var sc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",b=class extends Error{code;constructor(n,t){super(Jr(n,t)),this.code=n}};function tv(e){return`NG0${Math.abs(e)}`}function Jr(e,n){return`${tv(e)}${n?": "+n:""}`}var Mn=globalThis;function B(e){for(let n in e)if(e[n]===B)return n;throw Error("")}function Df(e,n){for(let t in n)n.hasOwnProperty(t)&&!e.hasOwnProperty(t)&&(e[t]=n[t])}function No(e){if(typeof e=="string")return e;if(Array.isArray(e))return`[${e.map(No).join(", ")}]`;if(e==null)return""+e;let n=e.overriddenName||e.name;if(n)return`${n}`;let t=e.toString();if(t==null)return""+t;let r=t.indexOf(`
`);return r>=0?t.slice(0,r):t}function ko(e,n){return e?n?`${e} ${n}`:e:n||""}var nv=B({__forward_ref__:B});function sr(e){return e.__forward_ref__=sr,e}function me(e){return ac(e)?e():e}function ac(e){return typeof e=="function"&&e.hasOwnProperty(nv)&&e.__forward_ref__===sr}function _(e){return{token:e.token,providedIn:e.providedIn||null,factory:e.factory,value:void 0}}function G(e){return{providers:e.providers||[],imports:e.imports||[]}}function Oo(e){return rv(e,Fo)}function rv(e,n){return e.hasOwnProperty(n)&&e[n]||null}function iv(e){let n=e?.[Fo]??null;return n||null}function qa(e){return e&&e.hasOwnProperty(xo)?e[xo]:null}var Fo=B({\u0275prov:B}),xo=B({\u0275inj:B}),g=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,t){this._desc=n,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=_({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function cc(e){return e&&!!e.\u0275providers}var ei=B({\u0275cmp:B}),ti=B({\u0275dir:B}),lc=B({\u0275pipe:B});var Qr=B({\u0275fac:B}),Sn=B({__NG_ELEMENT_ID__:B}),yf=B({__NG_ENV_ID__:B});function tn(e){return uc(e,"@Component"),e[ei]||null}function dc(e){return uc(e,"@Directive"),e[ti]||null}function wf(e){return uc(e,"@Pipe"),e[lc]||null}function uc(e,n){if(e==null)throw new b(-919,!1)}function fc(e){return typeof e=="string"?e:e==null?"":String(e)}var Cf=B({ngErrorCode:B}),ov=B({ngErrorMessage:B}),sv=B({ngTokenPath:B});function pc(e,n){return Ef("",-200,n)}function Po(e,n){throw new b(-201,!1)}function Ef(e,n,t){let r=new b(n,e);return r[Cf]=n,r[ov]=e,t&&(r[sv]=t),r}function av(e){return e[Cf]}var Za;function If(){return Za}function Te(e){let n=Za;return Za=e,n}function hc(e,n,t){let r=Oo(e);if(r&&r.providedIn=="root")return r.value===void 0?r.value=r.factory():r.value;if(t&8)return null;if(n!==void 0)return n;Po(e,"")}var cv={},Cn=cv,lv="__NG_DI_FLAG__",Qa=class{injector;constructor(n){this.injector=n}retrieve(n,t){let r=En(t)||0;try{return this.injector.get(n,r&8?null:Cn,r)}catch(i){if(ir(i))return i;throw i}}};function dv(e,n=0){let t=Mo();if(t===void 0)throw new b(-203,!1);if(t===null)return hc(e,void 0,n);{let r=uv(n),i=t.retrieve(e,r);if(ir(i)){if(r.optional)return null;throw i}return i}}function x(e,n=0){return(If()||dv)(me(e),n)}function f(e,n){return x(e,En(n))}function En(e){return typeof e>"u"||typeof e=="number"?e:0|(e.optional&&8)|(e.host&&1)|(e.self&&2)|(e.skipSelf&&4)}function uv(e){return{optional:!!(e&8),host:!!(e&1),self:!!(e&2),skipSelf:!!(e&4)}}function Ya(e){let n=[];for(let t=0;t<e.length;t++){let r=me(e[t]);if(Array.isArray(r)){if(r.length===0)throw new b(900,!1);let i,o=0;for(let s=0;s<r.length;s++){let a=r[s],c=fv(a);typeof c=="number"?c===-1?i=a.token:o|=c:i=a}n.push(x(i,o))}else n.push(x(r))}return n}function fv(e){return e[lv]}function Xt(e,n){let t=e.hasOwnProperty(Qr);return t?e[Qr]:null}function Tf(e,n,t){if(e.length!==n.length)return!1;for(let r=0;r<e.length;r++){let i=e[r],o=n[r];if(t&&(i=t(i),o=t(o)),o!==i)return!1}return!0}function Mf(e){return e.flat(Number.POSITIVE_INFINITY)}function Lo(e,n){e.forEach(t=>Array.isArray(t)?Lo(t,n):n(t))}function mc(e,n,t){n>=e.length?e.push(t):e.splice(n,0,t)}function ni(e,n){return n>=e.length-1?e.pop():e.splice(n,1)[0]}function Sf(e,n){let t=[];for(let r=0;r<e;r++)t.push(n);return t}function xf(e,n,t,r){let i=e.length;if(i==n)e.push(t,r);else if(i===1)e.push(r,e[0]),e[0]=t;else{for(i--,e.push(e[i-1],e[i]);i>n;){let o=i-2;e[i]=e[o],i--}e[n]=t,e[n+1]=r}}function jo(e,n,t){let r=ar(e,n);return r>=0?e[r|1]=t:(r=~r,xf(e,r,n,t)),r}function Bo(e,n){let t=ar(e,n);if(t>=0)return e[t|1]}function ar(e,n){return pv(e,n,1)}function pv(e,n,t){let r=0,i=e.length>>t;for(;i!==r;){let o=r+(i-r>>1),s=e[o<<t];if(n===s)return o<<t;s>n?i=o:r=o+1}return~(i<<t)}var nn={},Me=[],cr=new g(""),gc=new g("",-1),yc=new g(""),Yr=class{get(n,t=Cn){if(t===Cn){let i=Ef("",-201);throw i.name="\u0275NotFound",i}return t}};function lr(e){return{\u0275providers:e}}function Rf(e){return lr([{provide:cr,multi:!0,useValue:e}])}function Af(...e){return{\u0275providers:vc(!0,e),\u0275fromNgModule:!0}}function vc(e,...n){let t=[],r=new Set,i,o=s=>{t.push(s)};return Lo(n,s=>{let a=s;Ro(a,o,[],r)&&(i||=[],i.push(a))}),i!==void 0&&Nf(i,o),t}function Nf(e,n){for(let t=0;t<e.length;t++){let{ngModule:r,providers:i}=e[t];bc(i,o=>{n(o,r)})}}function Ro(e,n,t,r){if(e=me(e),!e)return!1;let i=null,o=qa(e),s=!o&&tn(e);if(!o&&!s){let c=e.ngModule;if(o=qa(c),o)i=c;else return!1}else{if(s&&!s.standalone)return!1;i=e}let a=r.has(i);if(s){if(a)return!1;if(r.add(i),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)Ro(l,n,t,r)}}else if(o){if(o.imports!=null&&!a){r.add(i);let l;Lo(o.imports,d=>{Ro(d,n,t,r)&&(l||=[],l.push(d))}),l!==void 0&&Nf(l,n)}if(!a){let l=Xt(i)||(()=>new i);n({provide:i,useFactory:l,deps:Me},i),n({provide:yc,useValue:i,multi:!0},i),n({provide:cr,useValue:()=>x(i),multi:!0},i)}let c=o.providers;if(c!=null&&!a){let l=e;bc(c,d=>{n(d,l)})}}else return!1;return i!==e&&e.providers!==void 0}function bc(e,n){for(let t of e)cc(t)&&(t=t.\u0275providers),Array.isArray(t)?bc(t,n):n(t)}var hv=B({provide:String,useValue:B});function kf(e){return e!==null&&typeof e=="object"&&hv in e}function mv(e){return!!(e&&e.useExisting)}function gv(e){return!!(e&&e.useFactory)}function In(e){return typeof e=="function"}function Of(e){return!!e.useClass}var ri=new g(""),So={},vf={},Wa;function dr(){return Wa===void 0&&(Wa=new Yr),Wa}var ye=class{},Tn=class extends ye{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,t,r,i){super(),this.parent=t,this.source=r,this.scopes=i,Xa(n,s=>this.processProvider(s)),this.records.set(gc,or(void 0,this)),i.has("environment")&&this.records.set(ye,or(void 0,this));let o=this.records.get(ri);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(yc,Me,{self:!0}))}retrieve(n,t){let r=En(t)||0;try{return this.get(n,Cn,r)}catch(i){if(ir(i))return i;throw i}}destroy(){Zr(this),this._destroyed=!0;let n=M(null);try{for(let r of this._ngOnDestroyHooks)r.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let r of t)r()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),M(n)}}onDestroy(n){return Zr(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Zr(this);let t=ht(this),r=Te(void 0),i;try{return n()}finally{ht(t),Te(r)}}get(n,t=Cn,r){if(Zr(this),n.hasOwnProperty(yf))return n[yf](this);let i=En(r),o,s=ht(this),a=Te(void 0);try{if(!(i&4)){let l=this.records.get(n);if(l===void 0){let d=Dv(n)&&Oo(n);d&&this.injectableDefInScope(d)?l=or(Ka(n),So):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,i)}let c=i&2?dr():this.parent;return t=i&8&&t===Cn?null:t,c.get(n,t)}catch(c){let l=av(c);throw l===-200||l===-201?new b(l,null):c}finally{Te(a),ht(s)}}resolveInjectorInitializers(){let n=M(null),t=ht(this),r=Te(void 0),i;try{let o=this.get(cr,Me,{self:!0});for(let s of o)s()}finally{ht(t),Te(r),M(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=me(n);let t=In(n)?n:me(n&&n.provide),r=vv(n);if(!In(n)&&n.multi===!0){let i=this.records.get(t);i||(i=or(void 0,So,!0),i.factory=()=>Ya(i.multi),this.records.set(t,i)),t=n,i.multi.push(n)}this.records.set(t,r)}hydrate(n,t,r){let i=M(null);try{if(t.value===vf)throw pc("");return t.value===So&&(t.value=vf,t.value=t.factory(void 0,r)),typeof t.value=="object"&&t.value&&_v(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{M(i)}}injectableDefInScope(n){if(!n.providedIn)return!1;let t=me(n.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(n){let t=this._onDestroyHooks.indexOf(n);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Ka(e){let n=Oo(e),t=n!==null?n.factory:Xt(e);if(t!==null)return t;if(e instanceof g)throw new b(-204,!1);if(e instanceof Function)return yv(e);throw new b(-204,!1)}function yv(e){if(e.length>0)throw new b(-204,!1);let t=iv(e);return t!==null?()=>t.factory(e):()=>new e}function vv(e){if(kf(e))return or(void 0,e.useValue);{let n=_c(e);return or(n,So)}}function _c(e,n,t){let r;if(In(e)){let i=me(e);return Xt(i)||Ka(i)}else if(kf(e))r=()=>me(e.useValue);else if(gv(e))r=()=>e.useFactory(...Ya(e.deps||[]));else if(mv(e))r=(i,o)=>x(me(e.useExisting),o!==void 0&&o&8?8:void 0);else{let i=me(e&&(e.useClass||e.provide));if(bv(e))r=()=>new i(...Ya(e.deps));else return Xt(i)||Ka(i)}return r}function Zr(e){if(e.destroyed)throw new b(-205,!1)}function or(e,n,t=!1){return{factory:e,value:n,multi:t?[]:void 0}}function bv(e){return!!e.deps}function _v(e){return e!==null&&typeof e=="object"&&typeof e.ngOnDestroy=="function"}function Dv(e){return typeof e=="function"||typeof e=="object"&&e.ngMetadataName==="InjectionToken"}function Xa(e,n){for(let t of e)Array.isArray(t)?Xa(t,n):t&&cc(t)?Xa(t.\u0275providers,n):n(t)}function ur(e,n){let t;e instanceof Tn?(Zr(e),t=e):t=new Qa(e);let r,i=ht(t),o=Te(void 0);try{return n()}finally{ht(i),Te(o)}}function Ff(){return If()!==void 0||Mo()!=null}var nt=0,w=1,C=2,pe=3,Ge=4,Se=5,fr=6,pr=7,De=8,rn=9,gt=10,Z=11,hr=12,Dc=13,xn=14,ke=15,on=16,Rn=17,yt=18,sn=19,wc=20,Nt=21,Vo=22,ii=23,Ve=24,An=25,mr=26,ee=27,Pf=1;var an=7,oi=8,Nn=9,we=10;function kt(e){return Array.isArray(e)&&typeof e[Pf]=="object"}function rt(e){return Array.isArray(e)&&e[Pf]===!0}function Cc(e){return(e.flags&4)!==0}function Ot(e){return e.componentOffset>-1}function gr(e){return(e.flags&1)===1}function vt(e){return!!e.template}function yr(e){return(e[C]&512)!==0}function kn(e){return(e[C]&256)===256}var Ec="svg",Lf="math";function We(e){for(;Array.isArray(e);)e=e[nt];return e}function Ic(e,n){return We(n[e])}function it(e,n){return We(n[e.index])}function Ho(e,n){return e.data[n]}function Tc(e,n){return e[n]}function Mc(e,n,t,r){t>=e.data.length&&(e.data[t]=null,e.blueprint[t]=null),n[t]=r}function qe(e,n){let t=n[e];return kt(t)?t:t[nt]}function jf(e){return(e[C]&4)===4}function Uo(e){return(e[C]&128)===128}function Bf(e){return rt(e[pe])}function bt(e,n){return n==null?null:e[n]}function Sc(e){e[Rn]=0}function xc(e){e[C]&1024||(e[C]|=1024,Uo(e)&&vr(e))}function Vf(e,n){for(;e>0;)n=n[xn],e--;return n}function si(e){return!!(e[C]&9216||e[Ve]?.dirty)}function $o(e){e[gt].changeDetectionScheduler?.notify(8),e[C]&64&&(e[C]|=1024),si(e)&&vr(e)}function vr(e){e[gt].changeDetectionScheduler?.notify(0);let n=Jt(e);for(;n!==null&&!(n[C]&8192||(n[C]|=8192,!Uo(n)));)n=Jt(n)}function Rc(e,n){if(kn(e))throw new b(911,!1);e[Nt]===null&&(e[Nt]=[]),e[Nt].push(n)}function Hf(e,n){if(e[Nt]===null)return;let t=e[Nt].indexOf(n);t!==-1&&e[Nt].splice(t,1)}function Jt(e){let n=e[pe];return rt(n)?n[pe]:n}function Ac(e){return e[pr]??=[]}function Nc(e){return e.cleanup??=[]}function Uf(e,n,t,r){let i=Ac(n);i.push(t),e.firstCreatePass&&Nc(e).push(r,i.length-1)}var R={lFrame:ep(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Ja=!1;function $f(){return R.lFrame.elementDepthCount}function zf(){R.lFrame.elementDepthCount++}function kc(){R.lFrame.elementDepthCount--}function zo(){return R.bindingsEnabled}function Oc(){return R.skipHydrationRootTNode!==null}function Fc(e){return R.skipHydrationRootTNode===e}function Pc(){R.skipHydrationRootTNode=null}function A(){return R.lFrame.lView}function ae(){return R.lFrame.tView}function he(){let e=Lc();for(;e!==null&&e.type===64;)e=e.parent;return e}function Lc(){return R.lFrame.currentTNode}function Gf(){let e=R.lFrame,n=e.currentTNode;return e.isParent?n:n.parent}function br(e,n){let t=R.lFrame;t.currentTNode=e,t.isParent=n}function jc(){return R.lFrame.isParent}function Bc(){R.lFrame.isParent=!1}function Wf(){return R.lFrame.contextLView}function Vc(){return Ja}function Hc(e){let n=Ja;return Ja=e,n}function qf(){let e=R.lFrame,n=e.bindingRootIndex;return n===-1&&(n=e.bindingRootIndex=e.tView.bindingStartIndex),n}function Zf(e){return R.lFrame.bindingIndex=e}function _r(){return R.lFrame.bindingIndex++}function Uc(e){let n=R.lFrame,t=n.bindingIndex;return n.bindingIndex=n.bindingIndex+e,t}function Qf(){return R.lFrame.inI18n}function Yf(e,n){let t=R.lFrame;t.bindingIndex=t.bindingRootIndex=e,Go(n)}function Kf(){return R.lFrame.currentDirectiveIndex}function Go(e){R.lFrame.currentDirectiveIndex=e}function Xf(e){let n=R.lFrame.currentDirectiveIndex;return n===-1?null:e[n]}function $c(){return R.lFrame.currentQueryIndex}function Wo(e){R.lFrame.currentQueryIndex=e}function wv(e){let n=e[w];return n.type===2?n.declTNode:n.type===1?e[Se]:null}function zc(e,n,t){if(t&4){let i=n,o=e;for(;i=i.parent,i===null&&!(t&1);)if(i=wv(o),i===null||(o=o[xn],i.type&10))break;if(i===null)return!1;n=i,e=o}let r=R.lFrame=Jf();return r.currentTNode=n,r.lView=e,!0}function qo(e){let n=Jf(),t=e[w];R.lFrame=n,n.currentTNode=t.firstChild,n.lView=e,n.tView=t,n.contextLView=e,n.bindingIndex=t.bindingStartIndex,n.inI18n=!1}function Jf(){let e=R.lFrame,n=e===null?null:e.child;return n===null?ep(e):n}function ep(e){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:e,child:null,inI18n:!1};return e!==null&&(e.child=n),n}function tp(){let e=R.lFrame;return R.lFrame=e.parent,e.currentTNode=null,e.lView=null,e}var Gc=tp;function Zo(){let e=tp();e.isParent=!0,e.tView=null,e.selectedIndex=-1,e.contextLView=null,e.elementDepthCount=0,e.currentDirectiveIndex=-1,e.currentNamespace=null,e.bindingRootIndex=-1,e.bindingIndex=-1,e.currentQueryIndex=0}function np(e){return(R.lFrame.contextLView=Vf(e,R.lFrame.contextLView))[De]}function cn(){return R.lFrame.selectedIndex}function ln(e){R.lFrame.selectedIndex=e}function Qo(){let e=R.lFrame;return Ho(e.tView,e.selectedIndex)}function Dr(){R.lFrame.currentNamespace=Ec}function Yo(){Cv()}function Cv(){R.lFrame.currentNamespace=null}function Wc(){return R.lFrame.currentNamespace}var rp=!0;function Ko(){return rp}function ai(e){rp=e}function ec(e,n=null,t=null,r){let i=ip(e,n,t,r);return i.resolveInjectorInitializers(),i}function ip(e,n=null,t=null,r,i=new Set){let o=[t||Me,Af(e)],s;return new Tn(o,n||dr(),s||null,i)}var le=class e{static THROW_IF_NOT_FOUND=Cn;static NULL=new Yr;static create(n,t){if(Array.isArray(n))return ec({name:""},t,n,"");{let r=n.name??"";return ec({name:r},n.parent,n.providers,r)}}static \u0275prov=_({token:e,providedIn:"any",factory:()=>x(gc)});static __NG_ELEMENT_ID__=-1},L=new g(""),Ft=(()=>{class e{static __NG_ELEMENT_ID__=Ev;static __NG_ENV_ID__=t=>t}return e})(),tc=class extends Ft{_lView;constructor(n){super(),this._lView=n}get destroyed(){return kn(this._lView)}onDestroy(n){let t=this._lView;return Rc(t,n),()=>Hf(t,n)}};function Ev(){return new tc(A())}var op=!1,sp=new g(""),On=(()=>{class e{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new At(!1);debugTaskTracker=f(sp,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new N(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=_({token:e,providedIn:"root",factory:()=>new e})}return e})(),nc=class extends J{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,Ff()&&(this.destroyRef=f(Ft,{optional:!0})??void 0,this.pendingTasks=f(On,{optional:!0})??void 0)}emit(n){let t=M(null);try{super.next(n)}finally{M(t)}}subscribe(n,t,r){let i=n,o=t||(()=>null),s=r;if(n&&typeof n=="object"){let c=n;i=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),i&&(i=this.wrapInTimeout(i)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:i,error:o,complete:s});return n instanceof X&&n.add(a),a}wrapInTimeout(n){return t=>{let r=this.pendingTasks?.add();setTimeout(()=>{try{n(t)}finally{r!==void 0&&this.pendingTasks?.remove(r)}})}}},ge=nc;function Ao(...e){}function qc(e){let n,t;function r(){e=Ao;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{e(),r()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{e(),r()})),()=>r()}function ap(e){return queueMicrotask(()=>e()),()=>{e=Ao}}var Zc="isAngularZone",Kr=Zc+"_ID",Iv=0,V=class e{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new ge(!1);onMicrotaskEmpty=new ge(!1);onStable=new ge(!1);onError=new ge(!1);constructor(n){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:r=!1,shouldCoalesceRunChangeDetection:i=!1,scheduleInRootZone:o=op}=n;if(typeof Zone>"u")throw new b(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!i&&r,s.shouldCoalesceRunChangeDetection=i,s.callbackScheduled=!1,s.scheduleInRootZone=o,Sv(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Zc)===!0}static assertInAngularZone(){if(!e.isInAngularZone())throw new b(909,!1)}static assertNotInAngularZone(){if(e.isInAngularZone())throw new b(909,!1)}run(n,t,r){return this._inner.run(n,t,r)}runTask(n,t,r,i){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+i,n,Tv,Ao,Ao);try{return o.runTask(s,t,r)}finally{o.cancelTask(s)}}runGuarded(n,t,r){return this._inner.runGuarded(n,t,r)}runOutsideAngular(n){return this._outer.run(n)}},Tv={};function Qc(e){if(e._nesting==0&&!e.hasPendingMicrotasks&&!e.isStable)try{e._nesting++,e.onMicrotaskEmpty.emit(null)}finally{if(e._nesting--,!e.hasPendingMicrotasks)try{e.runOutsideAngular(()=>e.onStable.emit(null))}finally{e.isStable=!0}}}function Mv(e){if(e.isCheckStableRunning||e.callbackScheduled)return;e.callbackScheduled=!0;function n(){qc(()=>{e.callbackScheduled=!1,rc(e),e.isCheckStableRunning=!0,Qc(e),e.isCheckStableRunning=!1})}e.scheduleInRootZone?Zone.root.run(()=>{n()}):e._outer.run(()=>{n()}),rc(e)}function Sv(e){let n=()=>{Mv(e)},t=Iv++;e._inner=e._inner.fork({name:"angular",properties:{[Zc]:!0,[Kr]:t,[Kr+t]:!0},onInvokeTask:(r,i,o,s,a,c)=>{if(xv(c))return r.invokeTask(o,s,a,c);try{return bf(e),r.invokeTask(o,s,a,c)}finally{(e.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||e.shouldCoalesceRunChangeDetection)&&n(),_f(e)}},onInvoke:(r,i,o,s,a,c,l)=>{try{return bf(e),r.invoke(o,s,a,c,l)}finally{e.shouldCoalesceRunChangeDetection&&!e.callbackScheduled&&!Rv(c)&&n(),_f(e)}},onHasTask:(r,i,o,s)=>{r.hasTask(o,s),i===o&&(s.change=="microTask"?(e._hasPendingMicrotasks=s.microTask,rc(e),Qc(e)):s.change=="macroTask"&&(e.hasPendingMacrotasks=s.macroTask))},onHandleError:(r,i,o,s)=>(r.handleError(o,s),e.runOutsideAngular(()=>e.onError.emit(s)),!1)})}function rc(e){e._hasPendingMicrotasks||(e.shouldCoalesceEventChangeDetection||e.shouldCoalesceRunChangeDetection)&&e.callbackScheduled===!0?e.hasPendingMicrotasks=!0:e.hasPendingMicrotasks=!1}function bf(e){e._nesting++,e.isStable&&(e.isStable=!1,e.onUnstable.emit(null))}function _f(e){e._nesting--,Qc(e)}var Xr=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new ge;onMicrotaskEmpty=new ge;onStable=new ge;onError=new ge;run(n,t,r){return n.apply(t,r)}runGuarded(n,t,r){return n.apply(t,r)}runOutsideAngular(n){return n()}runTask(n,t,r,i){return n.apply(t,r)}};function xv(e){return cp(e,"__ignore_ng_zone__")}function Rv(e){return cp(e,"__scheduler_tick__")}function cp(e,n){return!Array.isArray(e)||e.length!==1?!1:e[0]?.data?.[n]===!0}var mt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Pt=new g("",{factory:()=>{let e=f(V),n=f(ye),t;return r=>{e.runOutsideAngular(()=>{n.destroyed&&!t?setTimeout(()=>{throw r}):(t??=n.get(mt),t.handleError(r))})}}}),lp={provide:cr,useValue:()=>{let e=f(mt,{optional:!0})},multi:!0},Av=new g("",{factory:()=>{let e=f(L).defaultView;if(!e)return;let n=f(Pt),t=o=>{n(o.reason),o.preventDefault()},r=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},i=()=>{e.addEventListener("unhandledrejection",t),e.addEventListener("error",r)};typeof Zone<"u"?Zone.root.run(i):i(),f(Ft).onDestroy(()=>{e.removeEventListener("error",r),e.removeEventListener("unhandledrejection",t)})}});function Yc(){return lr([Rf(()=>{f(Av)})])}function ci(e,n){let[t,r,i]=Ma(e,n?.equal),o=t,s=o[ft];return o.set=r,o.update=i,o.asReadonly=dp.bind(o),o}function dp(){let e=this[ft];if(e.readonlyFn===void 0){let n=()=>this();n[ft]=e,e.readonlyFn=n}return e.readonlyFn}var Xo=(()=>{class e{view;node;constructor(t,r){this.view=t,this.node=r}static __NG_ELEMENT_ID__=Nv}return e})();function Nv(){return new Xo(A(),he())}var en=class{},li=new g("",{factory:()=>!0});var Kc=new g(""),Jo=(()=>{class e{internalPendingTasks=f(On);scheduler=f(en);errorHandler=f(Pt);add(){let t=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(t)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(t))}}run(t){let r=this.add();t().catch(this.errorHandler).finally(r)}static \u0275prov=_({token:e,providedIn:"root",factory:()=>new e})}return e})(),Xc=(()=>{class e{static \u0275prov=_({token:e,providedIn:"root",factory:()=>new ic})}return e})(),ic=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let t=n.zone,r=this.queues.get(t);r.has(n)&&(r.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let t=n.zone;this.queues.has(t)||this.queues.set(t,new Set);let r=this.queues.get(t);r.has(n)||r.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[t,r]of this.queues)t===null?n||=this.flushQueue(r):n||=t.run(()=>this.flushQueue(r));n||(this.dirtyEffectCount=0)}}flushQueue(n){let t=!1;for(let r of n)r.dirty&&(this.dirtyEffectCount--,t=!0,r.run());return t}},oc=class{[ft];constructor(n){this[ft]=n}destroy(){this[ft].destroy()}};function _i(e){return{toString:e}.toString()}function $v(e){return typeof e=="function"}function jp(e,n,t,r){n!==null?n.applyValueToInputSignal(n,r):e[t]=r}var os=class{previousValue;currentValue;firstChange;constructor(n,t,r){this.previousValue=n,this.currentValue=t,this.firstChange=r}isFirstChange(){return this.firstChange}},jt=(()=>{let e=()=>Bp;return e.ngInherit=!0,e})();function Bp(e){return e.type.prototype.ngOnChanges&&(e.setInput=Gv),zv}function zv(){let e=Hp(this),n=e?.current;if(n){let t=e.previous;if(t===nn)e.previous=n;else for(let r in n)t[r]=n[r];e.current=null,this.ngOnChanges(n)}}function Gv(e,n,t,r,i){let o=this.declaredInputs[r],s=Hp(e)||Wv(e,{previous:nn,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new os(l&&l.currentValue,t,c===nn),jp(e,n,i,t)}var Vp="__ngSimpleChanges__";function Hp(e){return e[Vp]||null}function Wv(e,n){return e[Vp]=n}var up=[];var F=function(e,n=null,t){for(let r=0;r<up.length;r++){let i=up[r];i(e,n,t)}},k=(function(e){return e[e.TemplateCreateStart=0]="TemplateCreateStart",e[e.TemplateCreateEnd=1]="TemplateCreateEnd",e[e.TemplateUpdateStart=2]="TemplateUpdateStart",e[e.TemplateUpdateEnd=3]="TemplateUpdateEnd",e[e.LifecycleHookStart=4]="LifecycleHookStart",e[e.LifecycleHookEnd=5]="LifecycleHookEnd",e[e.OutputStart=6]="OutputStart",e[e.OutputEnd=7]="OutputEnd",e[e.BootstrapApplicationStart=8]="BootstrapApplicationStart",e[e.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",e[e.BootstrapComponentStart=10]="BootstrapComponentStart",e[e.BootstrapComponentEnd=11]="BootstrapComponentEnd",e[e.ChangeDetectionStart=12]="ChangeDetectionStart",e[e.ChangeDetectionEnd=13]="ChangeDetectionEnd",e[e.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",e[e.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",e[e.AfterRenderHooksStart=16]="AfterRenderHooksStart",e[e.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",e[e.ComponentStart=18]="ComponentStart",e[e.ComponentEnd=19]="ComponentEnd",e[e.DeferBlockStateStart=20]="DeferBlockStateStart",e[e.DeferBlockStateEnd=21]="DeferBlockStateEnd",e[e.DynamicComponentStart=22]="DynamicComponentStart",e[e.DynamicComponentEnd=23]="DynamicComponentEnd",e[e.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",e[e.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",e})(k||{});function qv(e,n,t){let{ngOnChanges:r,ngOnInit:i,ngDoCheck:o}=n.type.prototype;if(r){let s=Bp(n);(t.preOrderHooks??=[]).push(e,s),(t.preOrderCheckHooks??=[]).push(e,s)}i&&(t.preOrderHooks??=[]).push(0-e,i),o&&((t.preOrderHooks??=[]).push(e,o),(t.preOrderCheckHooks??=[]).push(e,o))}function Up(e,n){for(let t=n.directiveStart,r=n.directiveEnd;t<r;t++){let o=e.data[t].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(e.contentHooks??=[]).push(-t,s),a&&((e.contentHooks??=[]).push(t,a),(e.contentCheckHooks??=[]).push(t,a)),c&&(e.viewHooks??=[]).push(-t,c),l&&((e.viewHooks??=[]).push(t,l),(e.viewCheckHooks??=[]).push(t,l)),d!=null&&(e.destroyHooks??=[]).push(t,d)}}function ts(e,n,t){$p(e,n,3,t)}function ns(e,n,t,r){(e[C]&3)===t&&$p(e,n,t,r)}function Jc(e,n){let t=e[C];(t&3)===n&&(t&=16383,t+=1,e[C]=t)}function $p(e,n,t,r){let i=r!==void 0?e[Rn]&65535:0,o=r??-1,s=n.length-1,a=0;for(let c=i;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],r!=null&&a>=r)break}else n[c]<0&&(e[Rn]+=65536),(a<o||o==-1)&&(Zv(e,t,n,c),e[Rn]=(e[Rn]&4294901760)+c+2),c++}function fp(e,n){F(k.LifecycleHookStart,e,n);let t=M(null);try{n.call(e)}finally{M(t),F(k.LifecycleHookEnd,e,n)}}function Zv(e,n,t,r){let i=t[r]<0,o=t[r+1],s=i?-t[r]:t[r],a=e[s];i?e[C]>>14<e[Rn]>>16&&(e[C]&3)===n&&(e[C]+=16384,fp(a,o)):fp(a,o)}var Cr=-1,Pn=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,t,r,i){this.factory=n,this.name=i,this.canSeeViewProviders=t,this.injectImpl=r}};function Qv(e){return(e.flags&8)!==0}function Yv(e){return(e.flags&16)!==0}function Kv(e,n,t){let r=0;for(;r<t.length;){let i=t[r];if(typeof i=="number"){if(i!==0)break;r++;let o=t[r++],s=t[r++],a=t[r++];e.setAttribute(n,s,a,o)}else{let o=i,s=t[++r];Xv(o)?e.setProperty(n,o,s):e.setAttribute(n,o,s),r++}}return r}function zp(e){return e===3||e===4||e===6}function Xv(e){return e.charCodeAt(0)===64}function Er(e,n){if(!(n===null||n.length===0))if(e===null||e.length===0)e=n.slice();else{let t=-1;for(let r=0;r<n.length;r++){let i=n[r];typeof i=="number"?t=i:t===0||(t===-1||t===2?pp(e,t,i,null,n[++r]):pp(e,t,i,null,null))}}return e}function pp(e,n,t,r,i){let o=0,s=e.length;if(n===-1)s=-1;else for(;o<e.length;){let a=e[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<e.length;){let a=e[o];if(typeof a=="number")break;if(a===t){i!==null&&(e[o+1]=i);return}o++,i!==null&&o++}s!==-1&&(e.splice(s,0,n),o=s+1),e.splice(o++,0,t),i!==null&&e.splice(o++,0,i)}function Gp(e){return e!==Cr}function ss(e){return e&32767}function Jv(e){return e>>16}function as(e,n){let t=Jv(e),r=n;for(;t>0;)r=r[xn],t--;return r}var dl=!0;function cs(e){let n=dl;return dl=e,n}var eb=256,Wp=eb-1,qp=5,tb=0,_t={};function nb(e,n,t){let r;typeof t=="string"?r=t.charCodeAt(0)||0:t.hasOwnProperty(Sn)&&(r=t[Sn]),r==null&&(r=t[Sn]=tb++);let i=r&Wp,o=1<<i;n.data[e+(i>>qp)]|=o}function ls(e,n){let t=Zp(e,n);if(t!==-1)return t;let r=n[w];r.firstCreatePass&&(e.injectorIndex=n.length,el(r.data,e),el(n,null),el(r.blueprint,null));let i=Fl(e,n),o=e.injectorIndex;if(Gp(i)){let s=ss(i),a=as(i,n),c=a[w].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=i,o}function el(e,n){e.push(0,0,0,0,0,0,0,0,n)}function Zp(e,n){return e.injectorIndex===-1||e.parent&&e.parent.injectorIndex===e.injectorIndex||n[e.injectorIndex+8]===null?-1:e.injectorIndex}function Fl(e,n){if(e.parent&&e.parent.injectorIndex!==-1)return e.parent.injectorIndex;let t=0,r=null,i=n;for(;i!==null;){if(r=Jp(i),r===null)return Cr;if(t++,i=i[xn],r.injectorIndex!==-1)return r.injectorIndex|t<<16}return Cr}function ul(e,n,t){nb(e,n,t)}function rb(e,n){if(n==="class")return e.classes;if(n==="style")return e.styles;let t=e.attrs;if(t){let r=t.length,i=0;for(;i<r;){let o=t[i];if(zp(o))break;if(o===0)i=i+2;else if(typeof o=="number")for(i++;i<r&&typeof t[i]=="string";)i++;else{if(o===n)return t[i+1];i=i+2}}}return null}function Qp(e,n,t){if(t&8||e!==void 0)return e;Po(n,"NodeInjector")}function Yp(e,n,t,r){if(t&8&&r===void 0&&(r=null),(t&3)===0){let i=e[rn],o=Te(void 0);try{return i?i.get(n,r,t&8):hc(n,r,t&8)}finally{Te(o)}}return Qp(r,n,t)}function Kp(e,n,t,r=0,i){if(e!==null){if(n[C]&2048&&!(r&2)){let s=ab(e,n,t,r,_t);if(s!==_t)return s}let o=Xp(e,n,t,r,_t);if(o!==_t)return o}return Yp(n,t,r,i)}function Xp(e,n,t,r,i){let o=ob(t);if(typeof o=="function"){if(!zc(n,e,r))return r&1?Qp(i,t,r):Yp(n,t,r,i);try{let s;if(s=o(r),s==null&&!(r&8))Po(t);else return s}finally{Gc()}}else if(typeof o=="number"){let s=null,a=Zp(e,n),c=Cr,l=r&1?n[ke][Se]:null;for((a===-1||r&4)&&(c=a===-1?Fl(e,n):n[a+8],c===Cr||!mp(r,!1)?a=-1:(s=n[w],a=ss(c),n=as(c,n)));a!==-1;){let d=n[w];if(hp(o,a,d.data)){let u=ib(a,n,t,s,r,l);if(u!==_t)return u}c=n[a+8],c!==Cr&&mp(r,n[w].data[a+8]===l)&&hp(o,a,n)?(s=d,a=ss(c),n=as(c,n)):a=-1}}return i}function ib(e,n,t,r,i,o){let s=n[w],a=s.data[e+8],c=r==null?Ot(a)&&dl:r!=s&&(a.type&3)!==0,l=i&1&&o===a,d=rs(a,s,t,c,l);return d!==null?hi(n,s,d,a,i):_t}function rs(e,n,t,r,i){let o=e.providerIndexes,s=n.data,a=o&1048575,c=e.directiveStart,l=e.directiveEnd,d=o>>20,u=r?a:a+d,h=i?a+d:l;for(let p=u;p<h;p++){let m=s[p];if(p<c&&t===m||p>=c&&m.type===t)return p}if(i){let p=s[c];if(p&&vt(p)&&p.type===t)return c}return null}function hi(e,n,t,r,i){let o=e[t],s=n.data;if(o instanceof Pn){let a=o;if(a.resolving)throw pc("");let c=cs(a.canSeeViewProviders);a.resolving=!0;let l=s[t].type||s[t],d,u=a.injectImpl?Te(a.injectImpl):null,h=zc(e,r,0);try{o=e[t]=a.factory(void 0,i,s,e,r),n.firstCreatePass&&t>=r.directiveStart&&qv(t,s[t],n)}finally{u!==null&&Te(u),cs(c),a.resolving=!1,Gc()}}return o}function ob(e){if(typeof e=="string")return e.charCodeAt(0)||0;let n=e.hasOwnProperty(Sn)?e[Sn]:void 0;return typeof n=="number"?n>=0?n&Wp:sb:n}function hp(e,n,t){let r=1<<e;return!!(t[n+(e>>qp)]&r)}function mp(e,n){return!(e&2)&&!(e&1&&n)}var Fn=class{_tNode;_lView;constructor(n,t){this._tNode=n,this._lView=t}get(n,t,r){return Kp(this._tNode,this._lView,n,En(r),t)}};function sb(){return new Fn(he(),A())}function Oe(e){return _i(()=>{let n=e.prototype.constructor,t=n[Qr]||fl(n),r=Object.prototype,i=Object.getPrototypeOf(e.prototype).constructor;for(;i&&i!==r;){let o=i[Qr]||fl(i);if(o&&o!==t)return o;i=Object.getPrototypeOf(i)}return o=>new o})}function fl(e){return ac(e)?()=>{let n=fl(me(e));return n&&n()}:Xt(e)}function ab(e,n,t,r,i){let o=e,s=n;for(;o!==null&&s!==null&&s[C]&2048&&!yr(s);){let a=Xp(o,s,t,r|2,_t);if(a!==_t)return a;let c=o.parent;if(!c){let l=s[wc];if(l){let d=l.get(t,_t,r&-5);if(d!==_t)return d}c=Jp(s),s=s[xn]}o=c}return i}function Jp(e){let n=e[w],t=n.type;return t===2?n.declTNode:t===1?e[Se]:null}function Pl(e){return rb(he(),e)}function cb(){return xr(he(),A())}function xr(e,n){return new U(it(e,n))}var U=(()=>{class e{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=cb}return e})();function lb(e){return e instanceof U?e.nativeElement:e}function db(){return this._results[Symbol.iterator]()}var mi=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new J}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,t){return this._results.reduce(n,t)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,t){this.dirty=!1;let r=Mf(n);(this._changesDetected=!Tf(this._results,r,t))&&(this._results=r,this.length=r.length,this.last=r[this.length-1],this.first=r[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=db};function eh(e){return(e.flags&128)===128}var Ll=(function(e){return e[e.OnPush=0]="OnPush",e[e.Eager=1]="Eager",e[e.Default=1]="Default",e})(Ll||{}),th=new Map,ub=0;function fb(){return ub++}function pb(e){th.set(e[sn],e)}function pl(e){th.delete(e[sn])}var gp="__ngContext__";function Ir(e,n){kt(n)?(e[gp]=n[sn],pb(n)):e[gp]=n}function nh(e){return ih(e[hr])}function rh(e){return ih(e[Ge])}function ih(e){for(;e!==null&&!rt(e);)e=e[Ge];return e}var hb;function jl(e){hb=e}var Rr=new g("",{factory:()=>mb}),mb="ng";var vs=new g(""),jn=new g("",{providedIn:"platform",factory:()=>"unknown"}),Bl=new g(""),Bn=new g("",{factory:()=>f(L).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var oh=!1,sh=new g("",{factory:()=>oh});var yp=new WeakMap;function gb(e,n){if(e==null||typeof e!="object")return;let t=yp.get(e);t||(t=new WeakSet,yp.set(e,t)),t.add(n)}var yb=(e,n,t,r)=>{};function vb(e,n,t,r){yb(e,n,t,r)}function bs(e){return(e.flags&32)===32}var bb=()=>null;function ah(e,n,t=!1){return bb(e,n,t)}function ch(e,n){let t=e.contentQueries;if(t!==null){let r=M(null);try{for(let i=0;i<t.length;i+=2){let o=t[i],s=t[i+1];if(s!==-1){let a=e.data[s];Wo(o),a.contentQueries(2,n[s],s)}}}finally{M(r)}}}function hl(e,n,t){Wo(0);let r=M(null);try{n(e,t)}finally{M(r)}}function Vl(e,n,t){if(Cc(n)){let r=M(null);try{let i=n.directiveStart,o=n.directiveEnd;for(let s=i;s<o;s++){let a=e.data[s];if(a.contentQueries){let c=t[s];a.contentQueries(1,c,s)}}}finally{M(r)}}}var at=(function(e){return e[e.Emulated=0]="Emulated",e[e.None=2]="None",e[e.ShadowDom=3]="ShadowDom",e[e.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",e})(at||{});var ml=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${sc})`}};function Hl(e){return e instanceof ml?e.changingThisBreaksApplicationSecurity:e}var _b=/^>|^->|<!--|-->|--!>|<!-$/g,Db=/(<|>)/g,wb="\u200B$1\u200B";function Cb(e){return e.replace(_b,n=>n.replace(Db,wb))}function Eb(e,n){return e.createText(n)}function Ib(e,n,t){e.setValue(n,t)}function Tb(e,n){return e.createComment(Cb(n))}function lh(e,n,t){return e.createElement(n,t)}function ds(e,n,t,r,i){e.insertBefore(n,t,r,i)}function dh(e,n,t){e.appendChild(n,t)}function vp(e,n,t,r,i){r!==null?ds(e,n,t,r,i):dh(e,n,t)}function Mb(e,n,t,r){e.removeChild(null,n,t,r)}function Sb(e,n,t){e.setAttribute(n,"style",t)}function xb(e,n,t){t===""?e.removeAttribute(n,"class"):e.setAttribute(n,"class",t)}function uh(e,n,t){let{mergedAttrs:r,classes:i,styles:o}=t;r!==null&&Kv(e,n,r),i!==null&&xb(e,n,i),o!==null&&Sb(e,n,o)}function Rb(e,n,t){let r=e.length;for(;;){let i=e.indexOf(n,t);if(i===-1)return i;if(i===0||e.charCodeAt(i-1)<=32){let o=n.length;if(i+o===r||e.charCodeAt(i+o)<=32)return i}t=i+1}}var fh="ng-template";function Ab(e,n,t,r){let i=0;if(r){for(;i<n.length&&typeof n[i]=="string";i+=2)if(n[i]==="class"&&Rb(n[i+1].toLowerCase(),t,0)!==-1)return!0}else if(Ul(e))return!1;if(i=n.indexOf(1,i),i>-1){let o;for(;++i<n.length&&typeof(o=n[i])=="string";)if(o.toLowerCase()===t)return!0}return!1}function Ul(e){return e.type===4&&e.value!==fh}function Nb(e,n,t){let r=e.type===4&&!t?fh:e.value;return n===r}function kb(e,n,t){let r=4,i=e.attrs,o=i!==null?Pb(i):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!ot(r)&&!ot(c))return!1;if(s&&ot(c))continue;s=!1,r=c|r&1;continue}if(!s)if(r&4){if(r=2|r&1,c!==""&&!Nb(e,c,t)||c===""&&n.length===1){if(ot(r))return!1;s=!0}}else if(r&8){if(i===null||!Ab(e,i,c,t)){if(ot(r))return!1;s=!0}}else{let l=n[++a],d=Ob(c,i,Ul(e),t);if(d===-1){if(ot(r))return!1;s=!0;continue}if(l!==""){let u;if(d>o?u="":u=i[d+1].toLowerCase(),r&2&&l!==u){if(ot(r))return!1;s=!0}}}}return ot(r)||s}function ot(e){return(e&1)===0}function Ob(e,n,t,r){if(n===null)return-1;let i=0;if(r||!t){let o=!1;for(;i<n.length;){let s=n[i];if(s===e)return i;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++i];for(;typeof a=="string";)a=n[++i];continue}else{if(s===4)break;if(s===0){i+=4;continue}}i+=o?1:2}return-1}else return Lb(n,e)}function ph(e,n,t=!1){for(let r=0;r<n.length;r++)if(kb(e,n[r],t))return!0;return!1}function Fb(e){let n=e.attrs;if(n!=null){let t=n.indexOf(5);if((t&1)===0)return n[t+1]}return null}function Pb(e){for(let n=0;n<e.length;n++){let t=e[n];if(zp(t))return n}return e.length}function Lb(e,n){let t=e.indexOf(4);if(t>-1)for(t++;t<e.length;){let r=e[t];if(typeof r=="number")return-1;if(r===n)return t;t++}return-1}function jb(e,n){e:for(let t=0;t<n.length;t++){let r=n[t];if(e.length===r.length){for(let i=0;i<e.length;i++)if(e[i]!==r[i])continue e;return!0}}return!1}function bp(e,n){return e?":not("+n.trim()+")":n}function Bb(e){let n=e[0],t=1,r=2,i="",o=!1;for(;t<e.length;){let s=e[t];if(typeof s=="string")if(r&2){let a=e[++t];i+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else r&8?i+="."+s:r&4&&(i+=" "+s);else i!==""&&!ot(s)&&(n+=bp(o,i),i=""),r=s,o=o||!ot(r);t++}return i!==""&&(n+=bp(o,i)),n}function Vb(e){return e.map(Bb).join(",")}function Hb(e){let n=[],t=[],r=1,i=2;for(;r<e.length;){let o=e[r];if(typeof o=="string")i===2?o!==""&&n.push(o,e[++r]):i===8&&t.push(o);else{if(!ot(i))break;i=o}r++}return t.length&&n.push(1,...t),n}var Ze={};function $l(e,n,t,r,i,o,s,a,c,l,d){let u=ee+r,h=u+i,p=Ub(u,h),m=typeof l=="function"?l():l;return p[w]={type:e,blueprint:p,template:t,queries:null,viewQuery:a,declTNode:n,data:p.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:m,incompleteFirstPass:!1,ssrId:d}}function Ub(e,n){let t=[];for(let r=0;r<n;r++)t.push(r<e?null:Ze);return t}function $b(e){let n=e.tView;return n===null||n.incompleteFirstPass?e.tView=$l(1,null,e.template,e.decls,e.vars,e.directiveDefs,e.pipeDefs,e.viewQuery,e.schemas,e.consts,e.id):n}function zl(e,n,t,r,i,o,s,a,c,l,d){let u=n.blueprint.slice();return u[nt]=i,u[C]=r|4|128|8|64|1024,(l!==null||e&&e[C]&2048)&&(u[C]|=2048),Sc(u),u[pe]=u[xn]=e,u[De]=t,u[gt]=s||e&&e[gt],u[Z]=a||e&&e[Z],u[rn]=c||e&&e[rn]||null,u[Se]=o,u[sn]=fb(),u[fr]=d,u[wc]=l,u[ke]=n.type==2?e[ke]:u,u}function zb(e,n,t){let r=it(n,e),i=$b(t),o=e[gt].rendererFactory,s=Gl(e,zl(e,i,null,hh(t),r,n,null,o.createRenderer(r,t),null,null,null));return e[n.index]=s}function hh(e){let n=16;return e.signals?n=4096:e.onPush&&(n=64),n}function mh(e,n,t,r){if(t===0)return-1;let i=n.length;for(let o=0;o<t;o++)n.push(r),e.blueprint.push(r),e.data.push(null);return i}function Gl(e,n){return e[hr]?e[Dc][Ge]=n:e[hr]=n,e[Dc]=n,n}function O(e=1){gh(ae(),A(),cn()+e,!1)}function gh(e,n,t,r){if(!r)if((n[C]&3)===3){let o=e.preOrderCheckHooks;o!==null&&ts(n,o,t)}else{let o=e.preOrderHooks;o!==null&&ns(n,o,0,t)}ln(t)}var _s=(function(e){return e[e.None=0]="None",e[e.SignalBased=1]="SignalBased",e[e.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",e})(_s||{});function gl(e,n,t,r){let i=M(null);try{let[o,s,a]=e.inputs[t],c=null;(s&_s.SignalBased)!==0&&(c=n[o][ft]),c!==null&&c.transformFn!==void 0?r=c.transformFn(r):a!==null&&(r=a.call(n,r)),e.setInput!==null?e.setInput(n,c,r,t,o):jp(n,c,o,r)}finally{M(i)}}var Dt=(function(e){return e[e.Important=1]="Important",e[e.DashCase=2]="DashCase",e})(Dt||{}),Gb;function Wl(e,n){return Gb(e,n)}var fA=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var yl=new WeakMap,ui=new WeakSet;function Wb(e,n){let t=yl.get(e);if(!t||t.length===0)return;let r=n.parentNode,i=n.previousSibling;for(let o=t.length-1;o>=0;o--){let s=t[o],a=s.parentNode;s===n?(t.splice(o,1),ui.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(i&&s===i||a&&r&&a!==r)&&(t.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function qb(e,n){let t=yl.get(e);t?t.includes(n)||t.push(n):yl.set(e,[n])}var Tr=new Set,Ds=(function(e){return e[e.CHANGE_DETECTION=0]="CHANGE_DETECTION",e[e.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",e})(Ds||{}),wt=new g(""),_p=new Set;function Ar(e){_p.has(e)||(_p.add(e),performance?.mark?.("mark_feature_usage",{detail:{feature:e}}))}var ql=(()=>{class e{impl=null;execute(){this.impl?.execute()}static \u0275prov=_({token:e,providedIn:"root",factory:()=>new e})}return e})(),yh=[0,1,2,3],vh=(()=>{class e{ngZone=f(V);scheduler=f(en);errorHandler=f(mt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){f(wt,{optional:!0})}execute(){let t=this.sequences.size>0;t&&F(k.AfterRenderHooksStart),this.executing=!0;for(let r of yh)for(let i of this.sequences)if(!(i.erroredOrDestroyed||!i.hooks[r]))try{i.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=i.hooks[r];return o(i.pipelinedValue)},i.snapshot))}catch(o){i.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let r of this.sequences)r.afterRun(),r.once&&(this.sequences.delete(r),r.destroy());for(let r of this.deferredRegistrations)this.sequences.add(r);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),t&&F(k.AfterRenderHooksEnd)}register(t){let{view:r}=t;r!==void 0?((r[An]??=[]).push(t),vr(r),r[C]|=8192):this.executing?this.deferredRegistrations.add(t):this.addSequence(t)}addSequence(t){this.sequences.add(t),this.scheduler.notify(7)}unregister(t){this.executing&&this.sequences.has(t)?(t.erroredOrDestroyed=!0,t.pipelinedValue=void 0,t.once=!0):(this.sequences.delete(t),this.deferredRegistrations.delete(t))}maybeTrace(t,r){return r?r.run(Ds.AFTER_NEXT_RENDER,t):t()}static \u0275prov=_({token:e,providedIn:"root",factory:()=>new e})}return e})(),us=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,t,r,i,o,s=null){this.impl=n,this.hooks=t,this.view=r,this.once=i,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[An];n&&(this.view[An]=n.filter(t=>t!==this))}};function un(e,n){let t=n?.injector??f(le);return Ar("NgAfterNextRender"),Qb(e,t,n,!0)}function Zb(e){return e instanceof Function?[void 0,void 0,e,void 0]:[e.earlyRead,e.write,e.mixedReadWrite,e.read]}function Qb(e,n,t,r){let i=n.get(ql);i.impl??=n.get(vh);let o=n.get(wt,null,{optional:!0}),s=t?.manualCleanup!==!0?n.get(Ft):null,a=n.get(Xo,null,{optional:!0}),c=new us(i.impl,Zb(e),a?.view,r,s,o?.snapshot(null));return i.impl.register(c),c}var Yb=new g("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:f(ye)})});function bh(e,n,t){let r=e.get(Yb);if(Array.isArray(n))for(let i of n)r.queue.add(i),t?.detachedLeaveAnimationFns?.push(i);else r.queue.add(n),t?.detachedLeaveAnimationFns?.push(n);r.scheduler&&r.scheduler(e)}function Kb(e,n){for(let[t,r]of n)bh(e,r.animateFns)}function Dp(e,n,t,r){let i=e?.[mr]?.enter;n!==null&&i&&i.has(t.index)&&Kb(r,i)}function wr(e,n,t,r,i,o,s,a){if(i!=null){let c,l=!1;rt(i)?c=i:kt(i)&&(l=!0,i=i[nt]);let d=We(i);e===0&&r!==null?(Dp(a,r,o,t),s==null?dh(n,r,d):ds(n,r,d,s||null,!0)):e===1&&r!==null?(Dp(a,r,o,t),ds(n,r,d,s||null,!0),Wb(o,d)):e===2?(a?.[mr]?.leave?.has(o.index)&&qb(o,d),ui.delete(d),wp(a,o,t,u=>{if(ui.has(d)){ui.delete(d);return}Mb(n,d,l,u)})):e===3&&(ui.delete(d),wp(a,o,t,()=>{n.destroyNode(d)})),c!=null&&c_(n,e,t,c,o,r,s)}}function Xb(e,n){_h(e,n),n[nt]=null,n[Se]=null}function Jb(e,n,t,r,i,o){r[nt]=i,r[Se]=n,ws(e,r,t,1,i,o)}function _h(e,n){n[gt].changeDetectionScheduler?.notify(9),ws(e,n,n[Z],2,null,null)}function e_(e){let n=e[hr];if(!n)return tl(e[w],e);for(;n;){let t=null;if(kt(n))t=n[hr];else{let r=n[we];r&&(t=r)}if(!t){for(;n&&!n[Ge]&&n!==e;)kt(n)&&tl(n[w],n),n=n[pe];n===null&&(n=e),kt(n)&&tl(n[w],n),t=n&&n[Ge]}n=t}}function Zl(e,n){let t=e[Nn],r=t.indexOf(n);t.splice(r,1)}function Ql(e,n){if(kn(n))return;let t=n[Z];t.destroyNode&&ws(e,n,t,3,null,null),e_(n)}function tl(e,n){if(kn(n))return;let t=M(null);try{n[C]&=-129,n[C]|=256,n[Ve]&&Yi(n[Ve]),r_(e,n),n_(e,n),n[w].type===1&&n[Z].destroy();let r=n[on];if(r!==null&&rt(n[pe])){r!==n[pe]&&Zl(r,n);let i=n[yt];i!==null&&i.detachView(e)}pl(n)}finally{M(t)}}function wp(e,n,t,r){let i=e?.[mr];if(i==null||i.leave==null||!i.leave.has(n.index))return r(!1);e&&Tr.add(e[sn]),bh(t,()=>{if(i.leave&&i.leave.has(n.index)){let s=i.leave.get(n.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:d}=l();a.push(d)}i.detachedLeaveAnimationFns=void 0}i.running=Promise.allSettled(a),t_(e,r)}else e&&Tr.delete(e[sn]),r(!1)},i)}function t_(e,n){let t=e[mr]?.running;if(t){t.then(()=>{e[mr].running=void 0,Tr.delete(e[sn]),n(!0)});return}n(!1)}function n_(e,n){let t=e.cleanup,r=n[pr];if(t!==null)for(let s=0;s<t.length-1;s+=2)if(typeof t[s]=="string"){let a=t[s+3];a>=0?r[a]():r[-a].unsubscribe(),s+=2}else{let a=r[t[s+1]];t[s].call(a)}r!==null&&(n[pr]=null);let i=n[Nt];if(i!==null){n[Nt]=null;for(let s=0;s<i.length;s++){let a=i[s];a()}}let o=n[ii];if(o!==null){n[ii]=null;for(let s of o)s.destroy()}}function r_(e,n){let t;if(e!=null&&(t=e.destroyHooks)!=null)for(let r=0;r<t.length;r+=2){let i=n[t[r]];if(!(i instanceof Pn)){let o=t[r+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=i[o[s]],c=o[s+1];F(k.LifecycleHookStart,a,c);try{c.call(a)}finally{F(k.LifecycleHookEnd,a,c)}}else{F(k.LifecycleHookStart,i,o);try{o.call(i)}finally{F(k.LifecycleHookEnd,i,o)}}}}}function Dh(e,n,t){return i_(e,n.parent,t)}function i_(e,n,t){let r=n;for(;r!==null&&r.type&168;)n=r,r=n.parent;if(r===null)return t[nt];if(Ot(r)){let{encapsulation:i}=e.data[r.directiveStart+r.componentOffset];if(i===at.None||i===at.Emulated)return null}return it(r,t)}function wh(e,n,t){return s_(e,n,t)}function o_(e,n,t){return e.type&40?it(e,t):null}var s_=o_,Cp;function Yl(e,n,t,r){let i=Dh(e,r,n),o=n[Z],s=r.parent||n[Se],a=wh(s,r,n);if(i!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)vp(o,i,t[c],a,!1);else vp(o,i,t,a,!1);Cp!==void 0&&Cp(o,r,n,t,i)}function fi(e,n){if(n!==null){let t=n.type;if(t&3)return it(n,e);if(t&4)return vl(-1,e[n.index]);if(t&8){let r=n.child;if(r!==null)return fi(e,r);{let i=e[n.index];return rt(i)?vl(-1,i):We(i)}}else{if(t&128)return fi(e,n.next);if(t&32)return Wl(n,e)()||We(e[n.index]);{let r=Ch(e,n);if(r!==null){if(Array.isArray(r))return r[0];let i=Jt(e[ke]);return fi(i,r)}else return fi(e,n.next)}}}return null}function Ch(e,n){if(n!==null){let r=e[ke][Se],i=n.projection;return r.projection[i]}return null}function vl(e,n){let t=we+e+1;if(t<n.length){let r=n[t],i=r[w].firstChild;if(i!==null)return fi(r,i)}return n[an]}function Kl(e,n,t,r,i,o,s){for(;t!=null;){let a=r[rn];if(t.type===128){t=t.next;continue}let c=r[t.index],l=t.type;if(s&&n===0&&(c&&Ir(We(c),r),t.flags|=2),!bs(t))if(l&8)Kl(e,n,t.child,r,i,o,!1),wr(n,e,a,i,c,t,o,r);else if(l&32){let d=Wl(t,r),u;for(;u=d();)wr(n,e,a,i,u,t,o,r);wr(n,e,a,i,c,t,o,r)}else l&16?Eh(e,n,r,t,i,o):wr(n,e,a,i,c,t,o,r);t=s?t.projectionNext:t.next}}function ws(e,n,t,r,i,o){Kl(t,r,e.firstChild,n,i,o,!1)}function a_(e,n,t){let r=n[Z],i=Dh(e,t,n),o=t.parent||n[Se],s=wh(o,t,n);Eh(r,0,n,t,i,s)}function Eh(e,n,t,r,i,o){let s=t[ke],c=s[Se].projection[r.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];wr(n,e,t[rn],i,d,r,o,t)}else{let l=c,d=s[pe];eh(r)&&(l.flags|=128),Kl(e,n,l,d,i,o,!0)}}function c_(e,n,t,r,i,o,s){let a=r[an],c=We(r);a!==c&&wr(n,e,t,o,a,i,s);for(let l=we;l<r.length;l++){let d=r[l];ws(d[w],d,e,n,o,a)}}function l_(e,n,t,r,i){if(n)i?e.addClass(t,r):e.removeClass(t,r);else{let o=r.indexOf("-")===-1?void 0:Dt.DashCase;i==null?e.removeStyle(t,r,o):(typeof i=="string"&&i.endsWith("!important")&&(i=i.slice(0,-10),o|=Dt.Important),e.setStyle(t,r,i,o))}}function Ih(e,n,t,r,i){let o=cn(),s=r&2;try{ln(-1),s&&n.length>ee&&gh(e,n,ee,!1);let a=s?k.TemplateUpdateStart:k.TemplateCreateStart;F(a,i,t),t(r,i)}finally{ln(o);let a=s?k.TemplateUpdateEnd:k.TemplateCreateEnd;F(a,i,t)}}function Cs(e,n,t){g_(e,n,t),(t.flags&64)===64&&y_(e,n,t)}function Di(e,n,t=it){let r=n.localNames;if(r!==null){let i=n.index+1;for(let o=0;o<r.length;o+=2){let s=r[o+1],a=s===-1?t(n,e):e[s];e[i++]=a}}}function d_(e,n,t,r){let o=r.get(sh,oh)||t===at.ShadowDom||t===at.ExperimentalIsolatedShadowDom,s=e.selectRootElement(n,o);if(s.tagName.toLowerCase()==="script")throw new b(905,!1);return u_(s),s}function u_(e){f_(e)}var f_=()=>null;function p_(e){return e==="class"?"className":e==="for"?"htmlFor":e==="formaction"?"formAction":e==="innerHtml"?"innerHTML":e==="readonly"?"readOnly":e==="tabindex"?"tabIndex":e}function h_(e,n,t,r,i,o){let s=n[w];if(td(e,s,n,t,r)){Ot(e)&&m_(n,e.index);return}e.type&3&&(t=p_(t)),Th(e,n,t,r,i,o)}function Th(e,n,t,r,i,o){if(e.type&3){let s=it(e,n);r=o!=null?o(r,e.value||"",t):r,i.setProperty(s,t,r)}else e.type&12}function m_(e,n){let t=qe(n,e);t[C]&16||(t[C]|=64)}function g_(e,n,t){let r=t.directiveStart,i=t.directiveEnd;Ot(t)&&zb(n,t,e.data[r+t.componentOffset]),e.firstCreatePass||ls(t,n);let o=t.initialInputs;for(let s=r;s<i;s++){let a=e.data[s],c=hi(n,e,s,t);if(Ir(c,n),o!==null&&D_(n,s-r,c,a,t,o),vt(a)){let l=qe(t.index,n);l[De]=hi(n,e,s,t)}}}function y_(e,n,t){let r=t.directiveStart,i=t.directiveEnd,o=t.index,s=Kf();try{ln(o);for(let a=r;a<i;a++){let c=e.data[a],l=n[a];Go(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&v_(c,l)}}finally{ln(-1),Go(s)}}function v_(e,n){e.hostBindings!==null&&e.hostBindings(1,n)}function Xl(e,n){let t=e.directiveRegistry,r=null;if(t)for(let i=0;i<t.length;i++){let o=t[i];ph(n,o.selectors,!1)&&(r??=[],vt(o)?r.unshift(o):r.push(o))}return r}function b_(e,n,t,r,i,o){let s=it(e,n);__(n[Z],s,o,e.value,t,r,i)}function __(e,n,t,r,i,o,s){if(o==null)e.removeAttribute(n,i,t);else{let a=s==null?fc(o):s(o,r||"",i);e.setAttribute(n,i,a,t)}}function D_(e,n,t,r,i,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];gl(r,t,c,l)}}function Jl(e,n,t,r,i){let o=ee+t,s=n[w],a=i(s,n,e,r,t);n[o]=a,br(e,!0);let c=e.type===2;return c?(uh(n[Z],a,e),($f()===0||gr(e))&&Ir(a,n),zf()):Ir(a,n),Ko()&&(!c||!bs(e))&&Yl(s,n,a,e),e}function ed(e){let n=e;return jc()?Bc():(n=n.parent,br(n,!1)),n}function w_(e,n){let t=e[rn];if(!t)return;let r;try{r=t.get(Pt,null)}catch{r=null}r?.(n)}function td(e,n,t,r,i){let o=e.inputs?.[r],s=e.hostDirectiveInputs?.[r],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],u=n.data[l];gl(u,t[l],d,i),a=!0}if(o)for(let c of o){let l=t[c],d=n.data[c];gl(d,l,r,i),a=!0}return a}function C_(e,n){let t=qe(n,e),r=t[w];E_(r,t);let i=t[nt];i!==null&&t[fr]===null&&(t[fr]=ah(i,t[rn])),F(k.ComponentStart);try{nd(r,t,t[De])}finally{F(k.ComponentEnd,t[De])}}function E_(e,n){for(let t=n.length;t<e.blueprint.length;t++)n.push(e.blueprint[t])}function nd(e,n,t){qo(n);try{let r=e.viewQuery;r!==null&&hl(1,r,t);let i=e.template;i!==null&&Ih(e,n,i,1,t),e.firstCreatePass&&(e.firstCreatePass=!1),n[yt]?.finishViewCreation(e),e.staticContentQueries&&ch(e,n),e.staticViewQueries&&hl(2,e.viewQuery,t);let o=e.components;o!==null&&I_(n,o)}catch(r){throw e.firstCreatePass&&(e.incompleteFirstPass=!0,e.firstCreatePass=!1),r}finally{n[C]&=-5,Zo()}}function I_(e,n){for(let t=0;t<n.length;t++)C_(e,n[t])}function rd(e,n,t,r){let i=M(null);try{let o=n.tView,a=e[C]&4096?4096:16,c=zl(e,o,t,a,null,n,null,null,r?.injector??null,r?.embeddedViewInjector??null,r?.dehydratedView??null),l=e[n.index];c[on]=l;let d=e[yt];return d!==null&&(c[yt]=d.createEmbeddedView(o)),nd(o,c,t),c}finally{M(i)}}function fs(e,n){return!n||n.firstChild===null||eh(e)}function gi(e,n,t,r,i=!1){for(;t!==null;){if(t.type===128){t=i?t.projectionNext:t.next;continue}let o=n[t.index];o!==null&&r.push(We(o)),rt(o)&&Mh(o,r);let s=t.type;if(s&8)gi(e,n,t.child,r);else if(s&32){let a=Wl(t,n),c;for(;c=a();)r.push(c)}else if(s&16){let a=Ch(n,t);if(Array.isArray(a))r.push(...a);else{let c=Jt(n[ke]);gi(c[w],c,a,r,!0)}}t=i?t.projectionNext:t.next}return r}function Mh(e,n){for(let t=we;t<e.length;t++){let r=e[t],i=r[w].firstChild;i!==null&&gi(r[w],r,i,n)}e[an]!==e[nt]&&n.push(e[an])}function Sh(e){if(e[An]!==null){for(let n of e[An])n.impl.addSequence(n);e[An].length=0}}var xh=[];function T_(e){return e[Ve]??M_(e)}function M_(e){let n=xh.pop()??Object.create(x_);return n.lView=e,n}function S_(e){e.lView[Ve]!==e&&(e.lView=null,xh.push(e))}var x_=Je(q({},Qi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{vr(e.lView)},consumerOnSignalRead(){this.lView[Ve]=this}});function R_(e){let n=e[Ve]??Object.create(A_);return n.lView=e,n}var A_=Je(q({},Qi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{let n=Jt(e.lView);for(;n&&!Rh(n[w]);)n=Jt(n);n&&xc(n)},consumerOnSignalRead(){this.lView[Ve]=this}});function Rh(e){return e.type!==2}function Ah(e){if(e[ii]===null)return;let n=!0;for(;n;){let t=!1;for(let r of e[ii])r.dirty&&(t=!0,r.zone===null||Zone.current===r.zone?r.run():r.zone.run(()=>r.run()));n=t&&!!(e[C]&8192)}}var N_=100;function Nh(e,n=0){let r=e[gt].rendererFactory,i=!1;i||r.begin?.();try{k_(e,n)}finally{i||r.end?.()}}function k_(e,n){let t=Vc();try{Hc(!0),bl(e,n);let r=0;for(;si(e);){if(r===N_)throw new b(103,!1);r++,bl(e,1)}}finally{Hc(t)}}function O_(e,n,t,r){if(kn(n))return;let i=n[C],o=!1,s=!1;qo(n);let a=!0,c=null,l=null;o||(Rh(e)?(l=T_(n),c=Ca(l)):Zi()===null?(a=!1,l=R_(n),c=Ca(l)):n[Ve]&&(Yi(n[Ve]),n[Ve]=null));try{Sc(n),Zf(e.bindingStartIndex),t!==null&&Ih(e,n,t,2,r);let d=(i&3)===3;if(!o)if(d){let p=e.preOrderCheckHooks;p!==null&&ts(n,p,null)}else{let p=e.preOrderHooks;p!==null&&ns(n,p,0,null),Jc(n,0)}if(s||F_(n),Ah(n),kh(n,0),e.contentQueries!==null&&ch(e,n),!o)if(d){let p=e.contentCheckHooks;p!==null&&ts(n,p)}else{let p=e.contentHooks;p!==null&&ns(n,p,1),Jc(n,1)}L_(e,n);let u=e.components;u!==null&&Fh(n,u,0);let h=e.viewQuery;if(h!==null&&hl(2,h,r),!o)if(d){let p=e.viewCheckHooks;p!==null&&ts(n,p)}else{let p=e.viewHooks;p!==null&&ns(n,p,2),Jc(n,2)}if(e.firstUpdatePass===!0&&(e.firstUpdatePass=!1),n[Vo]){for(let p of n[Vo])p();n[Vo]=null}o||(Sh(n),n[C]&=-73)}catch(d){throw o||vr(n),d}finally{l!==null&&(Eu(l,c),a&&S_(l)),Zo()}}function kh(e,n){for(let t=nh(e);t!==null;t=rh(t))for(let r=we;r<t.length;r++){let i=t[r];Oh(i,n)}}function F_(e){for(let n=nh(e);n!==null;n=rh(n)){if(!(n[C]&2))continue;let t=n[Nn];for(let r=0;r<t.length;r++){let i=t[r];xc(i)}}}function P_(e,n,t){F(k.ComponentStart);let r=qe(n,e);try{Oh(r,t)}finally{F(k.ComponentEnd,r[De])}}function Oh(e,n){Uo(e)&&bl(e,n)}function bl(e,n){let r=e[w],i=e[C],o=e[Ve],s=!!(n===0&&i&16);if(s||=!!(i&64&&n===0),s||=!!(i&1024),s||=!!(o?.dirty&&Ea(o)),s||=!1,o&&(o.dirty=!1),e[C]&=-9217,s)O_(r,e,r.template,e[De]);else if(i&8192){let a=M(null);try{Ah(e),kh(e,1);let c=r.components;c!==null&&Fh(e,c,1),Sh(e)}finally{M(a)}}}function Fh(e,n,t){for(let r=0;r<n.length;r++)P_(e,n[r],t)}function L_(e,n){let t=e.hostBindingOpCodes;if(t!==null)try{for(let r=0;r<t.length;r++){let i=t[r];if(i<0)ln(~i);else{let o=i,s=t[++r],a=t[++r];Yf(s,o);let c=n[o];F(k.HostBindingsUpdateStart,c);try{a(2,c)}finally{F(k.HostBindingsUpdateEnd,c)}}}}finally{ln(-1)}}function id(e,n){let t=Vc()?64:1088;for(e[gt].changeDetectionScheduler?.notify(n);e;){e[C]|=t;let r=Jt(e);if(yr(e)&&!r)return e;e=r}return null}function Ph(e,n,t,r){return[e,!0,0,n,null,r,null,t,null,null]}function j_(e,n){let t=we+n;if(t<e.length)return e[t]}function od(e,n,t,r=!0){let i=n[w];if(V_(i,n,e,t),r){let s=vl(t,e),a=n[Z],c=a.parentNode(e[an]);c!==null&&Jb(i,e[Se],a,n,c,s)}let o=n[fr];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function B_(e,n){let t=ps(e,n);return t!==void 0&&Ql(t[w],t),t}function ps(e,n){if(e.length<=we)return;let t=we+n,r=e[t];if(r){let i=r[on];i!==null&&i!==e&&Zl(i,r),n>0&&(e[t-1][Ge]=r[Ge]);let o=ni(e,we+n);Xb(r[w],r);let s=o[yt];s!==null&&s.detachView(o[w]),r[pe]=null,r[Ge]=null,r[C]&=-129}return r}function V_(e,n,t,r){let i=we+r,o=t.length;r>0&&(t[i-1][Ge]=n),r<o-we?(n[Ge]=t[i],mc(t,we+r,n)):(t.push(n),n[Ge]=null),n[pe]=t;let s=n[on];s!==null&&t!==s&&Lh(s,n);let a=n[yt];a!==null&&a.insertView(e),$o(n),n[C]|=128}function Lh(e,n){let t=e[Nn],r=n[pe];if(kt(r))e[C]|=2;else{let i=r[pe][ke];n[ke]!==i&&(e[C]|=2)}t===null?e[Nn]=[n]:t.push(n)}var dn=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,t=n[w];return gi(t,n,t.firstChild,[])}constructor(n,t){this._lView=n,this._cdRefInjectingView=t}get context(){return this._lView[De]}set context(n){this._lView[De]=n}get destroyed(){return kn(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[pe];if(rt(n)){let t=n[oi],r=t?t.indexOf(this):-1;r>-1&&(ps(n,r),ni(t,r))}this._attachedToViewContainer=!1}Ql(this._lView[w],this._lView)}onDestroy(n){Rc(this._lView,n)}markForCheck(){id(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[C]&=-129}reattach(){$o(this._lView),this._lView[C]|=128}detectChanges(){this._lView[C]|=1024,Nh(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new b(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=yr(this._lView),t=this._lView[on];t!==null&&!n&&Zl(t,this._lView),_h(this._lView[w],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new b(902,!1);this._appRef=n;let t=yr(this._lView),r=this._lView[on];r!==null&&!t&&Lh(r,this._lView),$o(this._lView)}};var ve=(()=>{class e{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=H_;constructor(t,r,i){this._declarationLView=t,this._declarationTContainer=r,this.elementRef=i}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,r){return this.createEmbeddedViewImpl(t,r)}createEmbeddedViewImpl(t,r,i){let o=rd(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:r,dehydratedView:i});return new dn(o)}}return e})();function H_(){return Es(he(),A())}function Es(e,n){return e.type&4?new ve(n,e,xr(e,n)):null}function Nr(e,n,t,r,i){let o=e.data[n];if(o===null)o=U_(e,n,t,r,i),Qf()&&(o.flags|=32);else if(o.type&64){o.type=t,o.value=r,o.attrs=i;let s=Gf();o.injectorIndex=s===null?-1:s.injectorIndex}return br(o,!0),o}function U_(e,n,t,r,i){let o=Lc(),s=jc(),a=s?o:o&&o.parent,c=e.data[n]=z_(e,a,t,n,r,i);return $_(e,c,o,s),c}function $_(e,n,t,r){e.firstChild===null&&(e.firstChild=n),t!==null&&(r?t.child==null&&n.parent!==null&&(t.child=n):t.next===null&&(t.next=n,n.prev=t))}function z_(e,n,t,r,i,o){let s=n?n.injectorIndex:-1,a=0;return Oc()&&(a|=128),{type:t,index:r,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:i,namespace:Wc(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var G_=()=>null,W_=()=>null;function _l(e,n){return G_(e,n)}function q_(e,n,t){return W_(e,n,t)}var jh=class{},Is=class{},Dl=class{resolveComponentFactory(n){throw new b(917,!1)}},Ts=class{static NULL=new Dl},He=class{},kr=(()=>{class e{destroyNode=null;static __NG_ELEMENT_ID__=()=>Z_()}return e})();function Z_(){let e=A(),n=he(),t=qe(n.index,e);return(kt(t)?t:e)[Z]}var Bh=(()=>{class e{static \u0275prov=_({token:e,providedIn:"root",factory:()=>null})}return e})();var is={},wl=class{injector;parentInjector;constructor(n,t){this.injector=n,this.parentInjector=t}get(n,t,r){let i=this.injector.get(n,is,r);return i!==is||t===is?i:this.parentInjector.get(n,t,r)}};function hs(e,n,t){let r=t?e.styles:null,i=t?e.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)i=ko(i,a);else if(o==2){let c=a,l=n[++s];r=ko(r,c+": "+l+";")}}t?e.styles=r:e.stylesWithoutHost=r,t?e.classes=i:e.classesWithoutHost=i}function xe(e,n=0){let t=A();if(t===null)return x(e,n);let r=he();return Kp(r,t,me(e),n)}function Vh(e,n,t,r,i){let o=r===null?null:{"":-1},s=i(e,t);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}K_(e,n,t,a,o,c,l)}o!==null&&r!==null&&Q_(t,r,o)}function Q_(e,n,t){let r=e.localNames=[];for(let i=0;i<n.length;i+=2){let o=t[n[i+1]];if(o==null)throw new b(-301,!1);r.push(n[i],o)}}function Y_(e,n,t){n.componentOffset=t,(e.components??=[]).push(n.index)}function K_(e,n,t,r,i,o,s){let a=r.length,c=null;for(let h=0;h<a;h++){let p=r[h];c===null&&vt(p)&&(c=p,Y_(e,t,h)),ul(ls(t,n),e,p.type)}rD(t,e.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let h=0;h<a;h++){let p=r[h];p.providersResolver&&p.providersResolver(p)}let l=!1,d=!1,u=mh(e,n,a,null);a>0&&(t.directiveToIndex=new Map);for(let h=0;h<a;h++){let p=r[h];if(t.mergedAttrs=Er(t.mergedAttrs,p.hostAttrs),J_(e,t,n,u,p),nD(u,p,i),s!==null&&s.has(p)){let[D,y]=s.get(p);t.directiveToIndex.set(p.type,[u,D+t.directiveStart,y+t.directiveStart])}else(o===null||!o.has(p))&&t.directiveToIndex.set(p.type,u);p.contentQueries!==null&&(t.flags|=4),(p.hostBindings!==null||p.hostAttrs!==null||p.hostVars!==0)&&(t.flags|=64);let m=p.type.prototype;!l&&(m.ngOnChanges||m.ngOnInit||m.ngDoCheck)&&((e.preOrderHooks??=[]).push(t.index),l=!0),!d&&(m.ngOnChanges||m.ngDoCheck)&&((e.preOrderCheckHooks??=[]).push(t.index),d=!0),u++}X_(e,t,o)}function X_(e,n,t){for(let r=n.directiveStart;r<n.directiveEnd;r++){let i=e.data[r];if(t===null||!t.has(i))Ep(0,n,i,r),Ep(1,n,i,r),Tp(n,r,!1);else{let o=t.get(i);Ip(0,n,o,r),Ip(1,n,o,r),Tp(n,r,!0)}}}function Ep(e,n,t,r){let i=e===0?t.inputs:t.outputs;for(let o in i)if(i.hasOwnProperty(o)){let s;e===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(r),Hh(n,o)}}function Ip(e,n,t,r){let i=e===0?t.inputs:t.outputs;for(let o in i)if(i.hasOwnProperty(o)){let s=i[o],a;e===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(r,o),Hh(n,s)}}function Hh(e,n){n==="class"?e.flags|=8:n==="style"&&(e.flags|=16)}function Tp(e,n,t){let{attrs:r,inputs:i,hostDirectiveInputs:o}=e;if(r===null||!t&&i===null||t&&o===null||Ul(e)){e.initialInputs??=[],e.initialInputs.push(null);return}let s=null,a=0;for(;a<r.length;){let c=r[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&i.hasOwnProperty(c)){let l=i[c];for(let d of l)if(d===n){s??=[],s.push(c,r[a+1]);break}}else if(t&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){s??=[],s.push(l[d+1],r[a+1]);break}}a+=2}e.initialInputs??=[],e.initialInputs.push(s)}function J_(e,n,t,r,i){e.data[r]=i;let o=i.factory||(i.factory=Xt(i.type,!0)),s=new Pn(o,vt(i),xe,null);e.blueprint[r]=s,t[r]=s,eD(e,n,r,mh(e,t,i.hostVars,Ze),i)}function eD(e,n,t,r,i){let o=i.hostBindings;if(o){let s=e.hostBindingOpCodes;s===null&&(s=e.hostBindingOpCodes=[]);let a=~n.index;tD(s)!=a&&s.push(a),s.push(t,r,o)}}function tD(e){let n=e.length;for(;n>0;){let t=e[--n];if(typeof t=="number"&&t<0)return t}return 0}function nD(e,n,t){if(t){if(n.exportAs)for(let r=0;r<n.exportAs.length;r++)t[n.exportAs[r]]=e;vt(n)&&(t[""]=e)}}function rD(e,n,t){e.flags|=1,e.directiveStart=n,e.directiveEnd=n+t,e.providerIndexes=n}function sd(e,n,t,r,i,o,s,a){let c=n[w],l=c.consts,d=bt(l,s),u=Nr(c,e,t,r,d);return o&&Vh(c,n,u,bt(l,a),i),u.mergedAttrs=Er(u.mergedAttrs,u.attrs),u.attrs!==null&&hs(u,u.attrs,!1),u.mergedAttrs!==null&&hs(u,u.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,u),u}function ad(e,n){Up(e,n),Cc(n)&&e.queries.elementEnd(n)}function iD(e,n,t,r,i,o){let s=n.consts,a=bt(s,i),c=Nr(n,e,t,r,a);if(c.mergedAttrs=Er(c.mergedAttrs,c.attrs),o!=null){let l=bt(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&hs(c,c.attrs,!1),c.mergedAttrs!==null&&hs(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}function cd(e){return $h(e)?Array.isArray(e)||!(e instanceof Map)&&Symbol.iterator in e:!1}function Uh(e,n){if(Array.isArray(e))for(let t=0;t<e.length;t++)n(e[t]);else{let t=e[Symbol.iterator](),r;for(;!(r=t.next()).done;)n(r.value)}}function $h(e){return e!==null&&(typeof e=="function"||typeof e=="object")}function oD(e,n,t){return e[n]=t}function Lt(e,n,t){if(t===Ze)return!1;let r=e[n];return Object.is(r,t)?!1:(e[n]=t,!0)}function sD(e,n,t,r){let i=Lt(e,n,t);return Lt(e,n+1,r)||i}function nl(e,n,t){return function r(i){let o=r.__ngNativeEl__;o!==void 0&&gb(i,o);let s=Ot(e)?qe(e.index,n):n;id(s,5);let a=n[De],c=Mp(n,a,t,i),l=r.__ngNextListenerFn__;for(;l;)c=Mp(n,a,l,i)&&c,l=l.__ngNextListenerFn__;return c}}function Mp(e,n,t,r){let i=M(null);try{return F(k.OutputStart,n,t),t(r)!==!1}catch(o){return w_(e,o),!1}finally{F(k.OutputEnd,n,t),M(i)}}function aD(e,n,t,r,i,o,s,a){let c=gr(e),l=!1,d=null;if(!r&&c&&(d=lD(n,t,o,e.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let u=it(e,t),h=r?r(u):u;vb(t,h,o,a),r||(a.__ngNativeEl__=u);let p=i.listen(h,o,a);if(!cD(o)){let m=r?D=>r(We(D[e.index])):e.index;zh(m,n,t,o,a,p,!1)}}return l}function cD(e){return e.startsWith("animation")||e.startsWith("transition")}function lD(e,n,t,r){let i=e.cleanup;if(i!=null)for(let o=0;o<i.length-1;o+=2){let s=i[o];if(s===t&&i[o+1]===r){let a=n[pr],c=i[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function zh(e,n,t,r,i,o,s){let a=n.firstCreatePass?Nc(n):null,c=Ac(t),l=c.length;c.push(i,o),a&&a.push(r,e,l,(l+1)*(s?-1:1))}function Sp(e,n,t,r,i,o){let s=n[t],a=n[w],l=a.data[t].outputs[r],u=s[l].subscribe(o);zh(e.index,a,n,i,o,u,!0)}var Cl=Symbol("BINDING");function Gh(e){return e.debugInfo?.className||e.type.name||null}var El=class extends Ts{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let t=tn(n);return new Mr(t,this.ngModule)}};function dD(e){return Object.keys(e).map(n=>{let[t,r,i]=e[n],o={propName:t,templateName:n,isSignal:(r&_s.SignalBased)!==0};return i&&(o.transform=i),o})}function uD(e){return Object.keys(e).map(n=>({propName:e[n],templateName:n}))}function fD(e,n,t){let r=n instanceof ye?n:n?.injector;return r&&e.getStandaloneInjector!==null&&(r=e.getStandaloneInjector(r)||r),r?new wl(t,r):t}function pD(e){let n=e.get(He,null);if(n===null)throw new b(407,!1);let t=e.get(Bh,null),r=e.get(en,null),i=e.get(wt,null,{optional:!0});return{rendererFactory:n,sanitizer:t,changeDetectionScheduler:r,ngReflect:!1,tracingService:i}}function hD(e,n){let t=Wh(e);return lh(n,t,t==="svg"?Ec:t==="math"?Lf:null)}function Wh(e){return(e.selectors[0][0]||"div").toLowerCase()}var Mr=class extends Is{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=dD(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=uD(this.componentDef.outputs),this.cachedOutputs}constructor(n,t){super(),this.componentDef=n,this.ngModule=t,this.componentType=n.type,this.selector=Vb(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!t}create(n,t,r,i,o,s){F(k.DynamicComponentStart);let a=M(null);try{let c=this.componentDef,l=fD(c,i||this.ngModule,n),d=pD(l),u=d.tracingService;return u&&u.componentCreate?u.componentCreate(Gh(c),()=>this.createComponentRef(d,l,t,r,o,s)):this.createComponentRef(d,l,t,r,o,s)}finally{M(a)}}createComponentRef(n,t,r,i,o,s){let a=this.componentDef,c=mD(i,a,s,o),l=n.rendererFactory.createRenderer(null,a),d=i?d_(l,i,a.encapsulation,t):hD(a,l),u=s?.some(xp)||o?.some(m=>typeof m!="function"&&m.bindings.some(xp)),h=zl(null,c,null,512|hh(a),null,null,n,l,t,null,ah(d,t,!0));h[ee]=d,qo(h);let p=null;try{let m=sd(ee,h,2,"#host",()=>c.directiveRegistry,!0,0);uh(l,d,m),Ir(d,h),Cs(c,h,m),Vl(c,m,h),ad(c,m),r!==void 0&&yD(m,this.ngContentSelectors,r),p=qe(m.index,h),h[De]=p[De],nd(c,h,null)}catch(m){throw p!==null&&pl(p),pl(h),m}finally{F(k.DynamicComponentEnd),Zo()}return new ms(this.componentType,h,!!u)}};function mD(e,n,t,r){let i=e?["ng-version","21.2.17"]:Hb(n.selectors[0]),o=null,s=null,a=0;if(t)for(let d of t)a+=d[Cl].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(r)for(let d=0;d<r.length;d++){let u=r[d];if(typeof u!="function")for(let h of u.bindings){a+=h[Cl].requiredVars;let p=d+1;h.create&&(h.targetIdx=p,(o??=[]).push(h)),h.update&&(h.targetIdx=p,(s??=[]).push(h))}}let c=[n];if(r)for(let d of r){let u=typeof d=="function"?d:d.type,h=dc(u);c.push(h)}return $l(0,null,gD(o,s),1,a,c,null,null,null,[i],null)}function gD(e,n){return!e&&!n?null:t=>{if(t&1&&e)for(let r of e)r.create();if(t&2&&n)for(let r of n)r.update()}}function xp(e){let n=e[Cl].kind;return n==="input"||n==="twoWay"}var ms=class extends jh{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,t,r){super(),this._rootLView=t,this._hasInputBindings=r,this._tNode=Ho(t[w],ee),this.location=xr(this._tNode,t),this.instance=qe(this._tNode.index,t)[De],this.hostView=this.changeDetectorRef=new dn(t,void 0),this.componentType=n}setInput(n,t){this._hasInputBindings;let r=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),t))return;let i=this._rootLView,o=td(r,i[w],i,n,t);this.previousInputValues.set(n,t);let s=qe(r.index,i);id(s,1)}get injector(){return new Fn(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function yD(e,n,t){let r=e.projection=[];for(let i=0;i<n.length;i++){let o=t[i];r.push(o!=null&&o.length?Array.from(o):null)}}var Fe=(()=>{class e{static __NG_ELEMENT_ID__=vD}return e})();function vD(){let e=he();return qh(e,A())}var Il=class e extends Fe{_lContainer;_hostTNode;_hostLView;constructor(n,t,r){super(),this._lContainer=n,this._hostTNode=t,this._hostLView=r}get element(){return xr(this._hostTNode,this._hostLView)}get injector(){return new Fn(this._hostTNode,this._hostLView)}get parentInjector(){let n=Fl(this._hostTNode,this._hostLView);if(Gp(n)){let t=as(n,this._hostLView),r=ss(n),i=t[w].data[r+8];return new Fn(i,t)}else return new Fn(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let t=Rp(this._lContainer);return t!==null&&t[n]||null}get length(){return this._lContainer.length-we}createEmbeddedView(n,t,r){let i,o;typeof r=="number"?i=r:r!=null&&(i=r.index,o=r.injector);let s=_l(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(t||{},o,s);return this.insertImpl(a,i,fs(this._hostTNode,s)),a}createComponent(n,t,r,i,o,s,a){let c=n&&!$v(n),l;if(c)l=t;else{let y=t||{};l=y.index,r=y.injector,i=y.projectableNodes,o=y.environmentInjector||y.ngModuleRef,s=y.directives,a=y.bindings}let d=c?n:new Mr(tn(n)),u=r||this.parentInjector;if(!o&&d.ngModule==null){let v=(c?u:this.parentInjector).get(ye,null);v&&(o=v)}let h=tn(d.componentType??{}),p=_l(this._lContainer,h?.id??null),m=p?.firstChild??null,D=d.create(u,i,m,o,s,a);return this.insertImpl(D.hostView,l,fs(this._hostTNode,p)),D}insert(n,t){return this.insertImpl(n,t,!0)}insertImpl(n,t,r){let i=n._lView;if(Bf(i)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=i[pe],l=new e(c,c[Se],c[pe]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(t),s=this._lContainer;return od(s,i,o,r),n.attachToViewContainerRef(),mc(rl(s),o,n),n}move(n,t){return this.insert(n,t)}indexOf(n){let t=Rp(this._lContainer);return t!==null?t.indexOf(n):-1}remove(n){let t=this._adjustIndex(n,-1),r=ps(this._lContainer,t);r&&(ni(rl(this._lContainer),t),Ql(r[w],r))}detach(n){let t=this._adjustIndex(n,-1),r=ps(this._lContainer,t);return r&&ni(rl(this._lContainer),t)!=null?new dn(r):null}_adjustIndex(n,t=0){return n??this.length+t}};function Rp(e){return e[oi]}function rl(e){return e[oi]||(e[oi]=[])}function qh(e,n){let t,r=n[e.index];return rt(r)?t=r:(t=Ph(r,n,null,e),n[e.index]=t,Gl(n,t)),_D(t,n,e,r),new Il(t,e,n)}function bD(e,n){let t=e[Z],r=t.createComment(""),i=it(n,e),o=t.parentNode(i);return ds(t,o,r,t.nextSibling(i),!1),r}var _D=CD,DD=()=>!1;function wD(e,n,t){return DD(e,n,t)}function CD(e,n,t,r){if(e[an])return;let i;t.type&8?i=We(r):i=bD(n,t),e[an]=i}var Tl=class e{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new e(this.queryList)}setDirty(){this.queryList.setDirty()}},Ml=class e{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let t=n.queries;if(t!==null){let r=n.contentQueries!==null?n.contentQueries[0]:t.length,i=[];for(let o=0;o<r;o++){let s=t.getByIndex(o),a=this.queries[s.indexInDeclarationView];i.push(a.clone())}return new e(i)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let t=0;t<this.queries.length;t++)ld(n,t).matches!==null&&this.queries[t].setDirty()}},gs=class{flags;read;predicate;constructor(n,t,r=null){this.flags=t,this.read=r,typeof n=="string"?this.predicate=AD(n):this.predicate=n}},Sl=class e{queries;constructor(n=[]){this.queries=n}elementStart(n,t){for(let r=0;r<this.queries.length;r++)this.queries[r].elementStart(n,t)}elementEnd(n){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(n)}embeddedTView(n){let t=null;for(let r=0;r<this.length;r++){let i=t!==null?t.length:0,o=this.getByIndex(r).embeddedTView(n,i);o&&(o.indexInDeclarationView=r,t!==null?t.push(o):t=[o])}return t!==null?new e(t):null}template(n,t){for(let r=0;r<this.queries.length;r++)this.queries[r].template(n,t)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},xl=class e{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,t=-1){this.metadata=n,this._declarationNodeIndex=t}elementStart(n,t){this.isApplyingToNode(t)&&this.matchTNode(n,t)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,t){this.elementStart(n,t)}embeddedTView(n,t){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,t),new e(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,r=n.parent;for(;r!==null&&r.type&8&&r.index!==t;)r=r.parent;return t===(r!==null?r.index:-1)}return this._appliesToNextNode}matchTNode(n,t){let r=this.metadata.predicate;if(Array.isArray(r))for(let i=0;i<r.length;i++){let o=r[i];this.matchTNodeWithReadOption(n,t,ED(t,o)),this.matchTNodeWithReadOption(n,t,rs(t,n,o,!1,!1))}else r===ve?t.type&4&&this.matchTNodeWithReadOption(n,t,-1):this.matchTNodeWithReadOption(n,t,rs(t,n,r,!1,!1))}matchTNodeWithReadOption(n,t,r){if(r!==null){let i=this.metadata.read;if(i!==null)if(i===U||i===Fe||i===ve&&t.type&4)this.addMatch(t.index,-2);else{let o=rs(t,n,i,!1,!1);o!==null&&this.addMatch(t.index,o)}else this.addMatch(t.index,r)}}addMatch(n,t){this.matches===null?this.matches=[n,t]:this.matches.push(n,t)}};function ED(e,n){let t=e.localNames;if(t!==null){for(let r=0;r<t.length;r+=2)if(t[r]===n)return t[r+1]}return null}function ID(e,n){return e.type&11?xr(e,n):e.type&4?Es(e,n):null}function TD(e,n,t,r){return t===-1?ID(n,e):t===-2?MD(e,n,r):hi(e,e[w],t,n)}function MD(e,n,t){if(t===U)return xr(n,e);if(t===ve)return Es(n,e);if(t===Fe)return qh(n,e)}function Zh(e,n,t,r){let i=n[yt].queries[r];if(i.matches===null){let o=e.data,s=t.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(TD(n,d,s[c+1],t.metadata.read))}}i.matches=a}return i.matches}function Rl(e,n,t,r){let i=e.queries.getByIndex(t),o=i.matches;if(o!==null){let s=Zh(e,n,i,t);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)r.push(s[a/2]);else{let l=o[a+1],d=n[-c];for(let u=we;u<d.length;u++){let h=d[u];h[on]===h[pe]&&Rl(h[w],h,l,r)}if(d[Nn]!==null){let u=d[Nn];for(let h=0;h<u.length;h++){let p=u[h];Rl(p[w],p,l,r)}}}}}return r}function SD(e,n){return e[yt].queries[n].queryList}function Qh(e,n,t){let r=new mi((t&4)===4);return Uf(e,n,r,r.destroy),(n[yt]??=new Ml).queries.push(new Tl(r))-1}function xD(e,n,t){let r=ae();return r.firstCreatePass&&(Yh(r,new gs(e,n,t),-1),(n&2)===2&&(r.staticViewQueries=!0)),Qh(r,A(),n)}function RD(e,n,t,r){let i=ae();if(i.firstCreatePass){let o=he();Yh(i,new gs(n,t,r),o.index),ND(i,e),(t&2)===2&&(i.staticContentQueries=!0)}return Qh(i,A(),t)}function AD(e){return e.split(",").map(n=>n.trim())}function Yh(e,n,t){e.queries===null&&(e.queries=new Sl),e.queries.track(new xl(n,t))}function ND(e,n){let t=e.contentQueries||(e.contentQueries=[]),r=t.length?t[t.length-1]:-1;n!==r&&t.push(e.queries.length-1,n)}function ld(e,n){return e.queries.getByIndex(n)}function kD(e,n){let t=e[w],r=ld(t,n);return r.crossesNgTemplate?Rl(t,e,n,[]):Zh(t,e,r,n)}var yi=class{};var vi=class extends yi{injector;componentFactoryResolver=new El(this);instance=null;constructor(n){super();let t=new Tn([...n.providers,{provide:yi,useValue:this},{provide:Ts,useValue:this.componentFactoryResolver}],n.parent||dr(),n.debugName,new Set(["environment"]));this.injector=t,n.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Kh(e,n,t=null){return new vi({providers:e,parent:n,debugName:t,runEnvironmentInitializers:!0}).injector}var OD=(()=>{class e{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let r=vc(!1,t.type),i=r.length>0?Kh([r],this._injector,""):null;this.cachedInjectors.set(t,i)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=_({token:e,providedIn:"environment",factory:()=>new e(x(ye))})}return e})();function P(e){return _i(()=>{let n=Xh(e),t=Je(q({},n),{decls:e.decls,vars:e.vars,template:e.template,consts:e.consts||null,ngContentSelectors:e.ngContentSelectors,onPush:e.changeDetection===Ll.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&e.dependencies||null,getStandaloneInjector:n.standalone?i=>i.get(OD).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:e.signals??!1,data:e.data||{},encapsulation:e.encapsulation||at.Emulated,styles:e.styles||Me,_:null,schemas:e.schemas||null,tView:null,id:""});n.standalone&&Ar("NgStandalone"),Jh(t);let r=e.dependencies;return t.directiveDefs=Ap(r,FD),t.pipeDefs=Ap(r,wf),t.id=jD(t),t})}function FD(e){return tn(e)||dc(e)}function Q(e){return _i(()=>({type:e.type,bootstrap:e.bootstrap||Me,declarations:e.declarations||Me,imports:e.imports||Me,exports:e.exports||Me,transitiveCompileScopes:null,schemas:e.schemas||null,id:e.id||null}))}function PD(e,n){if(e==null)return nn;let t={};for(let r in e)if(e.hasOwnProperty(r)){let i=e[r],o,s,a,c;Array.isArray(i)?(a=i[0],o=i[1],s=i[2]??o,c=i[3]||null):(o=i,s=i,a=_s.None,c=null),t[o]=[r,a,c],n[o]=s}return t}function LD(e){if(e==null)return nn;let n={};for(let t in e)e.hasOwnProperty(t)&&(n[e[t]]=t);return n}function I(e){return _i(()=>{let n=Xh(e);return Jh(n),n})}function dd(e){return{type:e.type,name:e.name,factory:null,pure:e.pure!==!1,standalone:e.standalone??!0,onDestroy:e.type.prototype.ngOnDestroy||null}}function Xh(e){let n={};return{type:e.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:e.hostBindings||null,hostVars:e.hostVars||0,hostAttrs:e.hostAttrs||null,contentQueries:e.contentQueries||null,declaredInputs:n,inputConfig:e.inputs||nn,exportAs:e.exportAs||null,standalone:e.standalone??!0,signals:e.signals===!0,selectors:e.selectors||Me,viewQuery:e.viewQuery||null,features:e.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:PD(e.inputs,n),outputs:LD(e.outputs),debugInfo:null}}function Jh(e){e.features?.forEach(n=>n(e))}function Ap(e,n){return e?()=>{let t=typeof e=="function"?e():e,r=[];for(let i of t){let o=n(i);o!==null&&r.push(o)}return r}:null}function jD(e){let n=0,t=typeof e.consts=="function"?"":e.consts,r=[e.selectors,e.ngContentSelectors,e.hostVars,e.hostAttrs,t,e.vars,e.decls,e.encapsulation,e.standalone,e.signals,e.exportAs,JSON.stringify(e.inputs),JSON.stringify(e.outputs),Object.getOwnPropertyNames(e.type.prototype),!!e.contentQueries,!!e.viewQuery];for(let o of r.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function BD(e){return Object.getPrototypeOf(e.prototype).constructor}function W(e){let n=BD(e.type),t=!0,r=[e];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let i,o=Object.hasOwn(n,ei)?n[ei]:void 0,s=Object.hasOwn(n,ti)?n[ti]:void 0;if(vt(e))i=o??s;else{if(o)throw new b(903,!1);i=s}if(i){if(t){r.push(i);let c=e;c.inputs=il(e.inputs),c.declaredInputs=il(e.declaredInputs),c.outputs=il(e.outputs);let l=i.hostBindings;l&&zD(e,l);let d=i.viewQuery,u=i.contentQueries;if(d&&UD(e,d),u&&$D(e,u),VD(e,i),Df(e.outputs,i.outputs),vt(i)&&i.data.animation){let h=e.data;h.animation=(h.animation||[]).concat(i.data.animation)}}let a=i.features;if(a)for(let c=0;c<a.length;c++){let l=a[c];l&&l.ngInherit&&l(e),l===W&&(t=!1)}}n=Object.getPrototypeOf(n)}HD(r)}function VD(e,n){for(let t in n.inputs){if(!n.inputs.hasOwnProperty(t)||e.inputs.hasOwnProperty(t))continue;let r=n.inputs[t];r!==void 0&&(e.inputs[t]=r,e.declaredInputs[t]=n.declaredInputs[t])}}function HD(e){let n=0,t=null;for(let r=e.length-1;r>=0;r--){let i=e[r];i.hostVars=n+=i.hostVars,i.hostAttrs=Er(i.hostAttrs,t=Er(t,i.hostAttrs))}}function il(e){return e===nn?{}:e===Me?[]:e}function UD(e,n){let t=e.viewQuery;t?e.viewQuery=(r,i)=>{n(r,i),t(r,i)}:e.viewQuery=n}function $D(e,n){let t=e.contentQueries;t?e.contentQueries=(r,i,o)=>{n(r,i,o),t(r,i,o)}:e.contentQueries=n}function zD(e,n){let t=e.hostBindings;t?e.hostBindings=(r,i)=>{n(r,i),t(r,i)}:e.hostBindings=n}function em(e,n,t,r,i,o,s,a){if(t.firstCreatePass){e.mergedAttrs=Er(e.mergedAttrs,e.attrs);let d=e.tView=$l(2,e,i,o,s,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,e),d.queries=t.queries.embeddedTView(e))}a&&(e.flags|=a),br(e,!1);let c=WD(t,n,e,r);Ko()&&Yl(t,n,c,e),Ir(c,n);let l=Ph(c,n,c,e);n[r+ee]=l,Gl(n,l),wD(l,e,n)}function GD(e,n,t,r,i,o,s,a,c,l,d){let u=t+ee,h;return n.firstCreatePass?(h=Nr(n,u,4,s||null,a||null),zo()&&Vh(n,e,h,bt(n.consts,l),Xl),Up(n,h)):h=n.data[u],em(h,e,n,t,r,i,o,c),gr(h)&&Cs(n,e,h),l!=null&&Di(e,h,d),h}function ud(e,n,t,r,i,o,s,a,c,l,d){let u=t+ee,h;if(n.firstCreatePass){if(h=Nr(n,u,4,s||null,a||null),l!=null){let p=bt(n.consts,l);h.localNames=[];for(let m=0;m<p.length;m+=2)h.localNames.push(p[m],-1)}}else h=n.data[u];return em(h,e,n,t,r,i,o,c),l!=null&&Di(e,h,d),h}function Pe(e,n,t,r,i,o,s,a){let c=A(),l=ae(),d=bt(l.consts,o);return GD(c,l,e,n,t,r,i,d,void 0,s,a),Pe}var WD=qD;function qD(e,n,t,r){return ai(!0),n[Z].createComment("")}var fd=new g("");function Ms(e){return!!e&&typeof e.then=="function"}function pd(e){return!!e&&typeof e.subscribe=="function"}var tm=new g("");var hd=(()=>{class e{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,r)=>{this.resolve=t,this.reject=r});appInits=f(tm,{optional:!0})??[];injector=f(le);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let i of this.appInits){let o=ur(this.injector,i);if(Ms(o))t.push(o);else if(pd(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});t.push(s)}}let r=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{r()}).catch(i=>{this.reject(i)}),t.length===0&&r(),this.initialized=!0}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),nm=new g("");function rm(){Ta(()=>{let e="";throw new b(600,e)})}function im(e){return e.isBoundToModule}var ZD=10;var Vn=(()=>{class e{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=f(Pt);afterRenderManager=f(ql);zonelessEnabled=f(li);rootEffectScheduler=f(Xc);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new J;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=f(On);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ze(t=>!t))}constructor(){f(wt,{optional:!0})}whenStable(){let t;return new Promise(r=>{t=this.isStable.subscribe({next:i=>{i&&r()}})}).finally(()=>{t.unsubscribe()})}_injector=f(ye);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,r){return this.bootstrapImpl(t,r)}bootstrapImpl(t,r,i=le.NULL){return this._injector.get(V).run(()=>{F(k.BootstrapComponentStart);let s=t instanceof Is;if(!this._injector.get(hd).done){let m="";throw new b(405,m)}let c;s?c=t:c=this._injector.get(Ts).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=im(c)?void 0:this._injector.get(yi),d=r||c.selector,u=c.create(i,[],d,l),h=u.location.nativeElement,p=u.injector.get(fd,null);return p?.registerApplication(h),u.onDestroy(()=>{this.detachView(u.hostView),pi(this.components,u),p?.unregisterApplication(h)}),this._loadComponent(u),F(k.BootstrapComponentEnd,u),u})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){F(k.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Ds.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw F(k.ChangeDetectionEnd),new b(101,!1);let t=M(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,M(t),this.afterTick.next(),F(k.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(He,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<ZD;){F(k.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{F(k.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let r=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:i}of this.allViews){if(!r&&!si(i))continue;let o=r&&!this.zonelessEnabled?0:1;Nh(i,o),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>si(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let r=t;this._views.push(r),r.attachToAppRef(this)}detachView(t){let r=t;pi(this._views,r),r.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(i){this.internalErrorHandler(i)}this.components.push(t),this._injector.get(nm,[]).forEach(i=>i(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>pi(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new b(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function pi(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function Re(e,n,t,r){let i=A(),o=_r();if(Lt(i,o,n)){let s=ae(),a=Qo();b_(a,i,e,n,t,r)}return Re}function Ct(e,n,t,r,i,o,s,a){Ar("NgControlFlow");let c=A(),l=ae(),d=bt(l.consts,o);return ud(c,l,e,n,t,r,i,d,256,s,a),md}function md(e,n,t,r,i,o,s,a){Ar("NgControlFlow");let c=A(),l=ae(),d=bt(l.consts,o);return ud(c,l,e,n,t,r,i,d,512,s,a),md}function Et(e,n){Ar("NgControlFlow");let t=A(),r=_r(),i=t[r]!==Ze?t[r]:-1,o=i!==-1?Np(t,ee+i):void 0,s=0;if(Lt(t,r,e)){let a=M(null);try{if(o!==void 0&&B_(o,s),e!==-1){let c=ee+e,l=Np(t,c),d=QD(t[w],c),u=q_(l,d,t),h=rd(t,d,n,{dehydratedView:u});od(l,h,s,fs(d,u))}}finally{M(a)}}else if(o!==void 0){let a=j_(o,s);a!==void 0&&(a[De]=n)}}function Np(e,n){return e[n]}function QD(e,n){return Ho(e,n)}function Ce(e,n,t){let r=A(),i=_r();if(Lt(r,i,n)){let o=ae(),s=Qo();h_(s,r,e,n,r[Z],t)}return Ce}function Al(e,n,t,r,i){td(n,e,t,i?"class":"style",r)}function E(e,n,t,r){let i=A(),o=i[w],s=e+ee,a=o.firstCreatePass?sd(s,i,2,n,Xl,zo(),t,r):o.data[s];if(Ot(a)){let c=i[gt].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(Gh(l),()=>(kp(e,n,i,a,r),E))}}return kp(e,n,i,a,r),E}function kp(e,n,t,r,i){if(Jl(r,t,e,n,om),gr(r)){let o=t[w];Cs(o,t,r),Vl(o,r,t)}i!=null&&Di(t,r)}function T(){let e=ae(),n=he(),t=ed(n);return e.firstCreatePass&&ad(e,t),Fc(t)&&Pc(),kc(),t.classesWithoutHost!=null&&Qv(t)&&Al(e,t,A(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&Yv(t)&&Al(e,t,A(),t.stylesWithoutHost,!1),T}function Le(e,n,t,r){return E(e,n,t,r),T(),Le}function Bt(e,n,t,r){let i=A(),o=i[w],s=e+ee,a=o.firstCreatePass?iD(s,o,2,n,t,r):o.data[s];return Jl(a,i,e,n,om),r!=null&&Di(i,a),Bt}function Vt(){let e=he(),n=ed(e);return Fc(n)&&Pc(),kc(),Vt}function Hn(e,n,t,r){return Bt(e,n,t,r),Vt(),Hn}var om=(e,n,t,r,i)=>(ai(!0),lh(n[Z],r,Wc()));function It(e,n,t){let r=A(),i=r[w],o=e+ee,s=i.firstCreatePass?sd(o,r,8,"ng-container",Xl,zo(),n,t):i.data[o];if(Jl(s,r,e,"ng-container",YD),gr(s)){let a=r[w];Cs(a,r,s),Vl(a,s,r)}return t!=null&&Di(r,s),It}function Tt(){let e=ae(),n=he(),t=ed(n);return e.firstCreatePass&&ad(e,t),Tt}function de(e,n,t){return It(e,n,t),Tt(),de}var YD=(e,n,t,r,i)=>(ai(!0),Tb(n[Z],""));function Ss(e,n,t){let r=A(),i=_r();if(Lt(r,i,n)){let o=ae(),s=Qo();Th(s,r,e,n,r[Z],t)}return Ss}var di=void 0;function KD(e){let n=Math.floor(Math.abs(e)),t=e.toString().replace(/^[^.]*\.?/,"").length;return n===1&&t===0?1:5}var XD=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],di,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],di,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",di,di,di],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",KD],ol=Object.create(null);function Ue(e){let n=JD(e),t=Op(n);if(t)return t;let r=n.split("-")[0];if(t=Op(r),t)return t;if(r==="en")return XD;throw new b(701,!1)}function Op(e){return e in ol||(ol[e]=Mn.ng&&Mn.ng.common&&Mn.ng.common.locales&&Mn.ng.common.locales[e]),ol[e]}var ce=(function(e){return e[e.LocaleId=0]="LocaleId",e[e.DayPeriodsFormat=1]="DayPeriodsFormat",e[e.DayPeriodsStandalone=2]="DayPeriodsStandalone",e[e.DaysFormat=3]="DaysFormat",e[e.DaysStandalone=4]="DaysStandalone",e[e.MonthsFormat=5]="MonthsFormat",e[e.MonthsStandalone=6]="MonthsStandalone",e[e.Eras=7]="Eras",e[e.FirstDayOfWeek=8]="FirstDayOfWeek",e[e.WeekendRange=9]="WeekendRange",e[e.DateFormat=10]="DateFormat",e[e.TimeFormat=11]="TimeFormat",e[e.DateTimeFormat=12]="DateTimeFormat",e[e.NumberSymbols=13]="NumberSymbols",e[e.NumberFormats=14]="NumberFormats",e[e.CurrencyCode=15]="CurrencyCode",e[e.CurrencySymbol=16]="CurrencySymbol",e[e.CurrencyName=17]="CurrencyName",e[e.Currencies=18]="Currencies",e[e.Directionality=19]="Directionality",e[e.PluralCase=20]="PluralCase",e[e.ExtraData=21]="ExtraData",e})(ce||{});function JD(e){return e.toLowerCase().replace(/_/g,"-")}var wi="en-US";var ew=wi;function sm(e){typeof e=="string"&&(ew=e.toLowerCase().replace(/_/g,"-"))}function Un(e,n,t){let r=A(),i=ae(),o=he();return tw(i,r,r[Z],o,e,n,t),Un}function tw(e,n,t,r,i,o,s){let a=!0,c=null;if((r.type&3||s)&&(c??=nl(r,n,o),aD(r,e,n,s,t,i,o,c)&&(a=!1)),a){let l=r.outputs?.[i],d=r.hostDirectiveOutputs?.[i];if(d&&d.length)for(let u=0;u<d.length;u+=2){let h=d[u],p=d[u+1];c??=nl(r,n,o),Sp(r,n,h,p,i,c)}if(l&&l.length)for(let u of l)c??=nl(r,n,o),Sp(r,n,u,i,i,c)}}function Qe(e=1){return np(e)}function nw(e,n){let t=null,r=Fb(e);for(let i=0;i<n.length;i++){let o=n[i];if(o==="*"){t=i;continue}if(r===null?ph(e,o,!0):jb(r,o))return i}return t}function je(e){let n=A()[ke][Se];if(!n.projection){let t=e?e.length:1,r=n.projection=Sf(t,null),i=r.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=e?nw(o,e):0;s!==null&&(i[s]?i[s].projectionNext=o:r[s]=o,i[s]=o)}o=o.next}}}function Y(e,n=0,t,r,i,o){let s=A(),a=ae(),c=r?e+1:null;c!==null&&ud(s,a,c,r,i,o,null,t);let l=Nr(a,ee+e,16,null,t||null);l.projection===null&&(l.projection=n),Bc();let u=!s[fr]||Oc();s[ke][Se].projection[l.projection]===null&&c!==null?rw(s,a,c):u&&!bs(l)&&a_(a,s,l)}function rw(e,n,t){let r=ee+t,i=n.data[r],o=e[r],s=_l(o,i.tView.ssrId),a=rd(e,i,void 0,{dehydratedView:s});od(o,a,0,fs(i,s))}function $n(e,n,t,r){return RD(e,n,t,r),$n}function fn(e,n,t){return xD(e,n,t),fn}function te(e){let n=A(),t=ae(),r=$c();Wo(r+1);let i=ld(t,r);if(e.dirty&&jf(n)===((i.metadata.flags&2)===2)){if(i.matches===null)e.reset([]);else{let o=kD(n,r);e.reset(o,lb),e.notifyOnChanges()}return!0}return!1}function ne(){return SD(A(),$c())}function gd(e){let n=Wf();return Tc(n,ee+e)}function es(e,n){return e<<17|n<<2}function Ln(e){return e>>17&32767}function iw(e){return(e&2)==2}function ow(e,n){return e&131071|n<<17}function Nl(e){return e|2}function Sr(e){return(e&131068)>>2}function sl(e,n){return e&-131069|n<<2}function sw(e){return(e&1)===1}function kl(e){return e|1}function aw(e,n,t,r,i,o){let s=o?n.classBindings:n.styleBindings,a=Ln(s),c=Sr(s);e[r]=t;let l=!1,d;if(Array.isArray(t)){let u=t;d=u[1],(d===null||ar(u,d)>0)&&(l=!0)}else d=t;if(i)if(c!==0){let h=Ln(e[a+1]);e[r+1]=es(h,a),h!==0&&(e[h+1]=sl(e[h+1],r)),e[a+1]=ow(e[a+1],r)}else e[r+1]=es(a,0),a!==0&&(e[a+1]=sl(e[a+1],r)),a=r;else e[r+1]=es(c,0),a===0?a=r:e[c+1]=sl(e[c+1],r),c=r;l&&(e[r+1]=Nl(e[r+1])),Fp(e,d,r,!0),Fp(e,d,r,!1),cw(n,d,e,r,o),s=es(a,c),o?n.classBindings=s:n.styleBindings=s}function cw(e,n,t,r,i){let o=i?e.residualClasses:e.residualStyles;o!=null&&typeof n=="string"&&ar(o,n)>=0&&(t[r+1]=kl(t[r+1]))}function Fp(e,n,t,r){let i=e[t+1],o=n===null,s=r?Ln(i):Sr(i),a=!1;for(;s!==0&&(a===!1||o);){let c=e[s],l=e[s+1];lw(c,n)&&(a=!0,e[s+1]=r?kl(l):Nl(l)),s=r?Ln(l):Sr(l)}a&&(e[t+1]=r?Nl(i):kl(i))}function lw(e,n){return e===null||n==null||(Array.isArray(e)?e[1]:e)===n?!0:Array.isArray(e)&&typeof n=="string"?ar(e,n)>=0:!1}var st={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function dw(e){return e.substring(st.key,st.keyEnd)}function uw(e){return fw(e),am(e,cm(e,0,st.textEnd))}function am(e,n){let t=st.textEnd;return t===n?-1:(n=st.keyEnd=pw(e,st.key=n,t),cm(e,n,t))}function fw(e){st.key=0,st.keyEnd=0,st.value=0,st.valueEnd=0,st.textEnd=e.length}function cm(e,n,t){for(;n<t&&e.charCodeAt(n)<=32;)n++;return n}function pw(e,n,t){for(;n<t&&e.charCodeAt(n)>32;)n++;return n}function Ht(e,n,t){return lm(e,n,t,!1),Ht}function K(e,n){return lm(e,n,null,!0),K}function zn(e){mw(Dw,hw,e,!0)}function hw(e,n){for(let t=uw(n);t>=0;t=am(n,t))jo(e,dw(n),!0)}function lm(e,n,t,r){let i=A(),o=ae(),s=Uc(2);if(o.firstUpdatePass&&um(o,e,s,r),n!==Ze&&Lt(i,s,n)){let a=o.data[cn()];fm(o,a,i,i[Z],e,i[s+1]=Cw(n,t),r,s)}}function mw(e,n,t,r){let i=ae(),o=Uc(2);i.firstUpdatePass&&um(i,null,o,r);let s=A();if(t!==Ze&&Lt(s,o,t)){let a=i.data[cn()];if(pm(a,r)&&!dm(i,o)){let c=r?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(t=ko(c,t||"")),Al(i,a,s,t,r)}else ww(i,a,s,s[Z],s[o+1],s[o+1]=_w(e,n,t),r,o)}}function dm(e,n){return n>=e.expandoStartIndex}function um(e,n,t,r){let i=e.data;if(i[t+1]===null){let o=i[cn()],s=dm(e,t);pm(o,r)&&n===null&&!s&&(n=!1),n=gw(i,o,n,r),aw(i,o,n,t,s,r)}}function gw(e,n,t,r){let i=Xf(e),o=r?n.residualClasses:n.residualStyles;if(i===null)(r?n.classBindings:n.styleBindings)===0&&(t=al(null,e,n,t,r),t=bi(t,n.attrs,r),o=null);else{let s=n.directiveStylingLast;if(s===-1||e[s]!==i)if(t=al(i,e,n,t,r),o===null){let c=yw(e,n,r);c!==void 0&&Array.isArray(c)&&(c=al(null,e,n,c[1],r),c=bi(c,n.attrs,r),vw(e,n,r,c))}else o=bw(e,n,r)}return o!==void 0&&(r?n.residualClasses=o:n.residualStyles=o),t}function yw(e,n,t){let r=t?n.classBindings:n.styleBindings;if(Sr(r)!==0)return e[Ln(r)]}function vw(e,n,t,r){let i=t?n.classBindings:n.styleBindings;e[Ln(i)]=r}function bw(e,n,t){let r,i=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<i;o++){let s=e[o].hostAttrs;r=bi(r,s,t)}return bi(r,n.attrs,t)}function al(e,n,t,r,i){let o=null,s=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<s&&(o=n[a],r=bi(r,o.hostAttrs,i),o!==e);)a++;return e!==null&&(t.directiveStylingLast=a),r}function bi(e,n,t){let r=t?1:2,i=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?i=s:i===r&&(Array.isArray(e)||(e=e===void 0?[]:["",e]),jo(e,s,t?!0:n[++o]))}return e===void 0?null:e}function _w(e,n,t){if(t==null||t==="")return Me;let r=[],i=Hl(t);if(Array.isArray(i))for(let o=0;o<i.length;o++)e(r,i[o],!0);else if(i instanceof Set)for(let o of i)e(r,o,!0);else if(typeof i=="object")for(let o in i)i.hasOwnProperty(o)&&e(r,o,i[o]);else typeof i=="string"&&n(r,i);return r}function Dw(e,n,t){let r=String(n);r!==""&&!r.includes(" ")&&jo(e,r,t)}function ww(e,n,t,r,i,o,s,a){i===Ze&&(i=Me);let c=0,l=0,d=0<i.length?i[0]:null,u=0<o.length?o[0]:null;for(;d!==null||u!==null;){let h=c<i.length?i[c+1]:void 0,p=l<o.length?o[l+1]:void 0,m=null,D;d===u?(c+=2,l+=2,h!==p&&(m=u,D=p)):u===null||d!==null&&d<u?(c+=2,m=d):(l+=2,m=u,D=p),m!==null&&fm(e,n,t,r,m,D,s,a),d=c<i.length?i[c]:null,u=l<o.length?o[l]:null}}function fm(e,n,t,r,i,o,s,a){if(!(n.type&3))return;let c=e.data,l=c[a+1],d=sw(l)?Pp(c,n,t,i,Sr(l),s):void 0;if(!ys(d)){ys(o)||iw(l)&&(o=Pp(c,null,t,i,a,s));let u=Ic(cn(),t);l_(r,s,u,i,o)}}function Pp(e,n,t,r,i,o){let s=n===null,a;for(;i>0;){let c=e[i],l=Array.isArray(c),d=l?c[1]:c,u=d===null,h=t[i+1];h===Ze&&(h=u?Me:void 0);let p=u?Bo(h,r):d===r?h:void 0;if(l&&!ys(p)&&(p=Bo(c,r)),ys(p)&&(a=p,s))return a;let m=e[i+1];i=s?Ln(m):Sr(m)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=Bo(c,r))}return a}function ys(e){return e!==void 0}function Cw(e,n){return e==null||e===""||(typeof n=="string"?e=e+n:typeof e=="object"&&(e=No(Hl(e)))),e}function pm(e,n){return(e.flags&(n?8:16))!==0}function be(e,n=""){let t=A(),r=ae(),i=e+ee,o=r.firstCreatePass?Nr(r,i,1,n,null):r.data[i],s=Ew(r,t,o,n);t[i]=s,Ko()&&Yl(r,t,s,o),br(o,!1)}var Ew=(e,n,t,r)=>(ai(!0),Eb(n[Z],r));function Iw(e,n,t,r=""){return Lt(e,_r(),t)?n+fc(t)+r:Ze}function Or(e){return Ut("",e),Or}function Ut(e,n,t){let r=A(),i=Iw(r,e,n,t);return i!==Ze&&Tw(r,cn(),i),Ut}function Tw(e,n,t){let r=Ic(n,e);Ib(e[Z],r,t)}function Lp(e,n,t){let r=ae();r.firstCreatePass&&hm(n,r.data,r.blueprint,vt(e),t)}function hm(e,n,t,r,i){if(e=me(e),Array.isArray(e))for(let o=0;o<e.length;o++)hm(e[o],n,t,r,i);else{let o=ae(),s=A(),a=he(),c=In(e)?e:me(e.provide),l=_c(e),d=a.providerIndexes&1048575,u=a.directiveStart,h=a.providerIndexes>>20;if(In(e)||!e.multi){let p=new Pn(l,i,xe,null),m=ll(c,n,i?d:d+h,u);m===-1?(ul(ls(a,s),o,c),cl(o,e,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,i&&(a.providerIndexes+=1048576),t.push(p),s.push(p)):(t[m]=p,s[m]=p)}else{let p=ll(c,n,d+h,u),m=ll(c,n,d,d+h),D=p>=0&&t[p],y=m>=0&&t[m];if(i&&!y||!i&&!D){ul(ls(a,s),o,c);let v=xw(i?Sw:Mw,t.length,i,r,l,e);!i&&y&&(t[m].providerFactory=v),cl(o,e,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,i&&(a.providerIndexes+=1048576),t.push(v),s.push(v)}else{let v=mm(t[i?m:p],l,!i&&r);cl(o,e,p>-1?p:m,v)}!i&&r&&y&&t[m].componentProviders++}}}function cl(e,n,t,r){let i=In(n),o=Of(n);if(i||o){let c=(o?me(n.useClass):n).prototype.ngOnDestroy;if(c){let l=e.destroyHooks||(e.destroyHooks=[]);if(!i&&n.multi){let d=l.indexOf(t);d===-1?l.push(t,[r,c]):l[d+1].push(r,c)}else l.push(t,c)}}}function mm(e,n,t){return t&&e.componentProviders++,e.multi.push(n)-1}function ll(e,n,t,r){for(let i=t;i<r;i++)if(n[i]===e)return i;return-1}function Mw(e,n,t,r,i){return Ol(this.multi,[])}function Sw(e,n,t,r,i){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=hi(r,r[w],this.providerFactory.index,i);s=c.slice(0,a),Ol(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],Ol(o,s);return s}function Ol(e,n){for(let t=0;t<e.length;t++){let r=e[t];n.push(r())}return n}function xw(e,n,t,r,i,o){let s=new Pn(e,t,xe,null);return s.multi=[],s.index=n,s.componentProviders=0,mm(s,i,r&&!t),s}function Ee(e,n){return t=>{t.providersResolver=(r,i)=>Lp(r,i?i(e):e,!1),n&&(t.viewProvidersResolver=(r,i)=>Lp(r,i?i(n):n,!0))}}function Rw(e,n){let t=e[n];return t===Ze?void 0:t}function Aw(e,n,t,r,i,o,s){let a=n+t;return sD(e,a,i,o)?oD(e,a+2,s?r.call(s,i,o):r(i,o)):Rw(e,a+2)}function yd(e,n){let t=ae(),r,i=e+ee;t.firstCreatePass?(r=Nw(n,t.pipeRegistry),t.data[i]=r,r.onDestroy&&(t.destroyHooks??=[]).push(i,r.onDestroy)):r=t.data[i];let o=r.factory||(r.factory=Xt(r.type,!0)),s,a=Te(xe);try{let c=cs(!1),l=o();return cs(c),Mc(t,A(),i,l),l}finally{Te(a)}}function Nw(e,n){if(n)for(let t=n.length-1;t>=0;t--){let r=n[t];if(e===r.name)return r}}function vd(e,n,t,r){let i=e+ee,o=A(),s=Tc(o,i);return kw(o,i)?Aw(o,qf(),n,s.transform,t,r,s):s.transform(t,r)}function kw(e,n){return e[w].data[n].pure}function bd(e,n){return Es(e,n)}var gm=(()=>{class e{applicationErrorHandler=f(Pt);appRef=f(Vn);taskService=f(On);ngZone=f(V);zonelessEnabled=f(li);tracing=f(wt,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new X;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Kr):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(f(Kc,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let r=this.useMicrotaskScheduler?ap:qc;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Kr+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(r){this.applicationErrorHandler(r)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function ym(){return[{provide:en,useExisting:gm},{provide:V,useClass:Xr},{provide:li,useValue:!0}]}function Ow(){return typeof $localize<"u"&&$localize.locale||wi}var Ci=new g("",{factory:()=>f(Ci,{optional:!0,skipSelf:!0})||Ow()});var Rs=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>Pl(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},Dm=(()=>{let e=new g("");return e.__NG_ELEMENT_ID__=n=>{let t=he();if(t===null)throw new b(-204,!1);if(t.type&2)return t.value;if(n&8)return null;throw new b(-204,!1)},e})();var _d=new g(""),Gw=new g("");function Ei(e){return!e.moduleRef}function Ww(e){let n=Ei(e)?e.r3Injector:e.moduleRef.injector,t=n.get(V);return t.run(()=>{Ei(e)?e.r3Injector.resolveInjectorInitializers():e.moduleRef.resolveInjectorInitializers();let r=n.get(Pt),i;if(t.runOutsideAngular(()=>{i=t.onError.subscribe({next:r})}),Ei(e)){let o=()=>n.destroy(),s=e.platformInjector.get(_d);s.add(o),n.onDestroy(()=>{i.unsubscribe(),s.delete(o)})}else{let o=()=>e.moduleRef.destroy(),s=e.platformInjector.get(_d);s.add(o),e.moduleRef.onDestroy(()=>{pi(e.allPlatformModules,e.moduleRef),i.unsubscribe(),s.delete(o)})}return Zw(r,t,()=>{let o=n.get(On),s=o.add(),a=n.get(hd);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(Ci,wi);if(sm(c||wi),!n.get(Gw,!0))return Ei(e)?n.get(Vn):(e.allPlatformModules.push(e.moduleRef),e.moduleRef);if(Ei(e)){let d=n.get(Vn);return e.rootComponent!==void 0&&d.bootstrap(e.rootComponent),d}else return qw?.(e.moduleRef,e.allPlatformModules),e.moduleRef}).finally(()=>{o.remove(s)})})})}var qw;function Zw(e,n,t){try{let r=t();return Ms(r)?r.catch(i=>{throw n.runOutsideAngular(()=>e(i)),i}):r}catch(r){throw n.runOutsideAngular(()=>e(r)),r}}var xs=null;function Qw(e=[],n){return le.create({name:n,providers:[{provide:ri,useValue:"platform"},{provide:_d,useValue:new Set([()=>xs=null])},...e]})}function Yw(e=[]){if(xs)return xs;let n=Qw(e);return xs=n,rm(),Kw(n),n}function Kw(e){let n=e.get(vs,null);ur(e,()=>{n?.forEach(t=>t())})}var Xw=1e4;var jP=Xw-1e3;var pn=(()=>{class e{static __NG_ELEMENT_ID__=Jw}return e})();function Jw(e){return eC(he(),A(),(e&16)===16)}function eC(e,n,t){if(Ot(e)&&!t){let r=qe(e.index,n);return new dn(r,r)}else if(e.type&175){let r=n[ke];return new dn(r,n)}return null}var Dd=class{supports(n){return cd(n)}create(n){return new wd(n)}},tC=(e,n)=>n,wd=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||tC}forEachItem(n){let t;for(t=this._itHead;t!==null;t=t._next)n(t)}forEachOperation(n){let t=this._itHead,r=this._removalsHead,i=0,o=null;for(;t||r;){let s=!r||t&&t.currentIndex<vm(r,i,o)?t:r,a=vm(s,i,o),c=s.currentIndex;if(s===r)i--,r=r._nextRemoved;else if(t=t._next,s.previousIndex==null)i++;else{o||(o=[]);let l=a-i,d=c-i;if(l!=d){for(let h=0;h<l;h++){let p=h<o.length?o[h]:o[h]=0,m=p+h;d<=m&&m<l&&(o[h]=p+1)}let u=s.previousIndex;o[u]=d-l}}a!==c&&n(s,a,c)}}forEachPreviousItem(n){let t;for(t=this._previousItHead;t!==null;t=t._nextPrevious)n(t)}forEachAddedItem(n){let t;for(t=this._additionsHead;t!==null;t=t._nextAdded)n(t)}forEachMovedItem(n){let t;for(t=this._movesHead;t!==null;t=t._nextMoved)n(t)}forEachRemovedItem(n){let t;for(t=this._removalsHead;t!==null;t=t._nextRemoved)n(t)}forEachIdentityChange(n){let t;for(t=this._identityChangesHead;t!==null;t=t._nextIdentityChange)n(t)}diff(n){if(n==null&&(n=[]),!cd(n))throw new b(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let t=this._itHead,r=!1,i,o,s;if(Array.isArray(n)){this.length=n.length;for(let a=0;a<this.length;a++)o=n[a],s=this._trackByFn(a,o),t===null||!Object.is(t.trackById,s)?(t=this._mismatch(t,o,s,a),r=!0):(r&&(t=this._verifyReinsertion(t,o,s,a)),Object.is(t.item,o)||this._addIdentityChange(t,o)),t=t._next}else i=0,Uh(n,a=>{s=this._trackByFn(i,a),t===null||!Object.is(t.trackById,s)?(t=this._mismatch(t,a,s,i),r=!0):(r&&(t=this._verifyReinsertion(t,a,s,i)),Object.is(t.item,a)||this._addIdentityChange(t,a)),t=t._next,i++}),this.length=i;return this._truncate(t),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,t,r,i){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(r,null),n!==null?(Object.is(n.item,t)||this._addIdentityChange(n,t),this._reinsertAfter(n,o,i)):(n=this._linkedRecords===null?null:this._linkedRecords.get(r,i),n!==null?(Object.is(n.item,t)||this._addIdentityChange(n,t),this._moveAfter(n,o,i)):n=this._addAfter(new Cd(t,r),o,i)),n}_verifyReinsertion(n,t,r,i){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(r,null);return o!==null?n=this._reinsertAfter(o,n._prev,i):n.currentIndex!=i&&(n.currentIndex=i,this._addToMoves(n,i)),n}_truncate(n){for(;n!==null;){let t=n._next;this._addToRemovals(this._unlink(n)),n=t}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,t,r){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let i=n._prevRemoved,o=n._nextRemoved;return i===null?this._removalsHead=o:i._nextRemoved=o,o===null?this._removalsTail=i:o._prevRemoved=i,this._insertAfter(n,t,r),this._addToMoves(n,r),n}_moveAfter(n,t,r){return this._unlink(n),this._insertAfter(n,t,r),this._addToMoves(n,r),n}_addAfter(n,t,r){return this._insertAfter(n,t,r),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,t,r){let i=t===null?this._itHead:t._next;return n._next=i,n._prev=t,i===null?this._itTail=n:i._prev=n,t===null?this._itHead=n:t._next=n,this._linkedRecords===null&&(this._linkedRecords=new As),this._linkedRecords.put(n),n.currentIndex=r,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let t=n._prev,r=n._next;return t===null?this._itHead=r:t._next=r,r===null?this._itTail=t:r._prev=t,n}_addToMoves(n,t){return n.previousIndex===t||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new As),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,t){return n.item=t,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},Cd=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,t){this.item=n,this.trackById=t}},Ed=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,t){let r;for(r=this._head;r!==null;r=r._nextDup)if((t===null||t<=r.currentIndex)&&Object.is(r.trackById,n))return r;return null}remove(n){let t=n._prevDup,r=n._nextDup;return t===null?this._head=r:t._nextDup=r,r===null?this._tail=t:r._prevDup=t,this._head===null}},As=class{map=new Map;put(n){let t=n.trackById,r=this.map.get(t);r||(r=new Ed,this.map.set(t,r)),r.add(n)}get(n,t){let r=n,i=this.map.get(r);return i?i.get(n,t):null}remove(n){let t=n.trackById;return this.map.get(t).remove(n)&&this.map.delete(t),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function vm(e,n,t){let r=e.previousIndex;if(r===null)return r;let i=0;return t&&r<t.length&&(i=t[r]),r+n+i}function bm(){return new $t([new Dd])}var $t=(()=>{class e{factories;static \u0275prov=_({token:e,providedIn:"root",factory:bm});constructor(t){this.factories=t}static create(t,r){if(r!=null){let i=r.factories.slice();t=t.concat(i)}return new e(t)}static extend(t){return{provide:e,useFactory:()=>{let r=f(e,{optional:!0,skipSelf:!0});return e.create(t,r||bm())}}}find(t){let r=this.factories.find(i=>i.supports(t));if(r!=null)return r;throw new b(901,!1)}}return e})();function wm(e){let{rootComponent:n,appProviders:t,platformProviders:r,platformRef:i}=e;F(k.BootstrapApplicationStart);try{let o=i?.injector??Yw(r),s=[ym(),lp,...t||[]],a=new vi({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return Ww({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{F(k.BootstrapApplicationEnd)}}function oe(e){return typeof e=="boolean"?e:e!=null&&e!=="false"}function hn(e,n=NaN){return!isNaN(parseFloat(e))&&!isNaN(Number(e))?Number(e):n}function Cm(e,n){let t=tn(e),r=n.elementInjector||dr();return new Mr(t).create(r,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var Em=null;function mn(){return Em}function Id(e){Em??=e}var Ii=class{},Ns=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:()=>f(Im),providedIn:"platform"})}return e})();var Im=(()=>{class e extends Ns{_location;_history;_doc=f(L);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return mn().getBaseHref(this._doc)}onPopState(t){let r=mn().getGlobalEventTarget(this._doc,"window");return r.addEventListener("popstate",t,!1),()=>r.removeEventListener("popstate",t)}onHashChange(t){let r=mn().getGlobalEventTarget(this._doc,"window");return r.addEventListener("hashchange",t,!1),()=>r.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,r,i){this._history.pushState(t,r,i)}replaceState(t,r,i){this._history.replaceState(t,r,i)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:()=>new e,providedIn:"platform"})}return e})();var Ie=(function(e){return e[e.Format=0]="Format",e[e.Standalone=1]="Standalone",e})(Ie||{}),H=(function(e){return e[e.Narrow=0]="Narrow",e[e.Abbreviated=1]="Abbreviated",e[e.Wide=2]="Wide",e[e.Short=3]="Short",e})(H||{}),Be=(function(e){return e[e.Short=0]="Short",e[e.Medium=1]="Medium",e[e.Long=2]="Long",e[e.Full=3]="Full",e})(Be||{}),Gt={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function Sm(e){return Ue(e)[ce.LocaleId]}function xm(e,n,t){let r=Ue(e),i=[r[ce.DayPeriodsFormat],r[ce.DayPeriodsStandalone]],o=Ye(i,n);return Ye(o,t)}function Rm(e,n,t){let r=Ue(e),i=[r[ce.DaysFormat],r[ce.DaysStandalone]],o=Ye(i,n);return Ye(o,t)}function Am(e,n,t){let r=Ue(e),i=[r[ce.MonthsFormat],r[ce.MonthsStandalone]],o=Ye(i,n);return Ye(o,t)}function Nm(e,n){let r=Ue(e)[ce.Eras];return Ye(r,n)}function Ti(e,n){let t=Ue(e);return Ye(t[ce.DateFormat],n)}function Mi(e,n){let t=Ue(e);return Ye(t[ce.TimeFormat],n)}function Si(e,n){let r=Ue(e)[ce.DateTimeFormat];return Ye(r,n)}function xi(e,n){let t=Ue(e),r=t[ce.NumberSymbols][n];if(typeof r>"u"){if(n===Gt.CurrencyDecimal)return t[ce.NumberSymbols][Gt.Decimal];if(n===Gt.CurrencyGroup)return t[ce.NumberSymbols][Gt.Group]}return r}function km(e){if(!e[ce.ExtraData])throw new b(2303,!1)}function Om(e){let n=Ue(e);return km(n),(n[ce.ExtraData][2]||[]).map(r=>typeof r=="string"?Td(r):[Td(r[0]),Td(r[1])])}function Fm(e,n,t){let r=Ue(e);km(r);let i=[r[ce.ExtraData][0],r[ce.ExtraData][1]],o=Ye(i,n)||[];return Ye(o,t)||[]}function Ye(e,n){for(let t=n;t>-1;t--)if(typeof e[t]<"u")return e[t];throw new b(2304,!1)}function Td(e){let[n,t]=e.split(":");return{hours:+n,minutes:+t}}var rC=/^(\d{4,})-?(\d\d)-?(\d\d)(?:T(\d\d)(?::?(\d\d)(?::?(\d\d)(?:\.(\d+))?)?)?(Z|([+-])(\d\d):?(\d\d))?)?$/,ks={},iC=/((?:[^BEGHLMOSWYZabcdhmswyz']+)|(?:'(?:[^']|'')*')|(?:G{1,5}|y{1,4}|Y{1,4}|M{1,5}|L{1,5}|w{1,2}|W{1}|d{1,2}|E{1,6}|c{1,6}|a{1,5}|b{1,5}|B{1,5}|h{1,2}|H{1,2}|m{1,2}|s{1,2}|S{1,3}|z{1,4}|Z{1,5}|O{1,4}))([\s\S]*)/,oC=256;function Pm(e,n,t,r){let i=mC(e);sC(n),n=zt(t,n)||n;let s=[],a;for(;n;)if(a=iC.exec(n),a){s=s.concat(a.slice(1));let d=s.pop();if(!d)break;n=d}else{s.push(n);break}let c=i.getTimezoneOffset();r&&(c=jm(r,c),i=hC(i,r));let l="";return s.forEach(d=>{let u=fC(d);l+=u?u(i,t,c):d==="''"?"'":d.replace(/(^'|'$)/g,"").replace(/''/g,"'")}),l}function sC(e){if(e.length>oC)throw new b(2300,!1)}function js(e,n,t){let r=new Date(0);return r.setFullYear(e,n,t),r.setHours(0,0,0),r}function zt(e,n){let t=Sm(e);if(ks[t]??={},ks[t][n])return ks[t][n];let r="";switch(n){case"shortDate":r=Ti(e,Be.Short);break;case"mediumDate":r=Ti(e,Be.Medium);break;case"longDate":r=Ti(e,Be.Long);break;case"fullDate":r=Ti(e,Be.Full);break;case"shortTime":r=Mi(e,Be.Short);break;case"mediumTime":r=Mi(e,Be.Medium);break;case"longTime":r=Mi(e,Be.Long);break;case"fullTime":r=Mi(e,Be.Full);break;case"short":let i=zt(e,"shortTime"),o=zt(e,"shortDate");r=Os(Si(e,Be.Short),[i,o]);break;case"medium":let s=zt(e,"mediumTime"),a=zt(e,"mediumDate");r=Os(Si(e,Be.Medium),[s,a]);break;case"long":let c=zt(e,"longTime"),l=zt(e,"longDate");r=Os(Si(e,Be.Long),[c,l]);break;case"full":let d=zt(e,"fullTime"),u=zt(e,"fullDate");r=Os(Si(e,Be.Full),[d,u]);break}return r&&(ks[t][n]=r),r}function Os(e,n){return n&&(e=e.replace(/\{([^}]+)}/g,function(t,r){return n!=null&&r in n?n[r]:t})),e}function ct(e,n,t="-",r,i){let o="";(e<0||i&&e<=0)&&(i?e=-e+1:(e=-e,o=t));let s=String(e);for(;s.length<n;)s="0"+s;return r&&(s=s.slice(s.length-n)),o+s}function aC(e,n){return ct(e,3).substring(0,n)}function ue(e,n,t=0,r=!1,i=!1){return function(o,s){let a=cC(e,o);if((t>0||a>-t)&&(a+=t),e===3)a===0&&t===-12&&(a=12);else if(e===6)return aC(a,n);let c=xi(s,Gt.MinusSign);return ct(a,n,c,r,i)}}function cC(e,n){switch(e){case 0:return n.getFullYear();case 1:return n.getMonth();case 2:return n.getDate();case 3:return n.getHours();case 4:return n.getMinutes();case 5:return n.getSeconds();case 6:return n.getMilliseconds();case 7:return n.getDay();default:throw new b(2301,!1)}}function $(e,n,t=Ie.Format,r=!1){return function(i,o){return lC(i,o,e,n,t,r)}}function lC(e,n,t,r,i,o){switch(t){case 2:return Am(n,i,r)[e.getMonth()];case 1:return Rm(n,i,r)[e.getDay()];case 0:let s=e.getHours(),a=e.getMinutes();if(o){let l=Om(n),d=Fm(n,i,r),u=l.findIndex(h=>{if(Array.isArray(h)){let[p,m]=h,D=s>=p.hours&&a>=p.minutes,y=s<m.hours||s===m.hours&&a<m.minutes;if(p.hours<m.hours){if(D&&y)return!0}else if(D||y)return!0}else if(h.hours===s&&h.minutes===a)return!0;return!1});if(u!==-1)return d[u]}return xm(n,i,r)[s<12?0:1];case 3:return Nm(n,r)[e.getFullYear()<=0?0:1];default:let c=t;throw new b(2302,!1)}}function Fs(e){return function(n,t,r){let i=-1*r,o=xi(t,Gt.MinusSign),s=i>0?Math.floor(i/60):Math.ceil(i/60);switch(e){case 0:return(i>=0?"+":"")+ct(s,2,o)+ct(Math.abs(i%60),2,o);case 1:return"GMT"+(i>=0?"+":"")+ct(s,1,o);case 2:return"GMT"+(i>=0?"+":"")+ct(s,2,o)+":"+ct(Math.abs(i%60),2,o);case 3:return r===0?"Z":(i>=0?"+":"")+ct(s,2,o)+":"+ct(Math.abs(i%60),2,o);default:throw new b(2310,!1)}}}var dC=0,Ls=4;function uC(e){let n=js(e,dC,1).getDay();return js(e,0,1+(n<=Ls?Ls:Ls+7)-n)}function Lm(e){let n=e.getDay(),t=n===0?-3:Ls-n;return js(e.getFullYear(),e.getMonth(),e.getDate()+t)}function Md(e,n=!1){return function(t,r){let i;if(n){let o=new Date(t.getFullYear(),t.getMonth(),1).getDay()-1,s=t.getDate();i=1+Math.floor((s+o)/7)}else{let o=Lm(t),s=uC(o.getFullYear()),a=o.getTime()-s.getTime();i=1+Math.round(a/6048e5)}return ct(i,e,xi(r,Gt.MinusSign))}}function Ps(e,n=!1){return function(t,r){let o=Lm(t).getFullYear();return ct(o,e,xi(r,Gt.MinusSign),n)}}var Sd={};function fC(e){if(Sd[e])return Sd[e];let n;switch(e){case"G":case"GG":case"GGG":n=$(3,H.Abbreviated);break;case"GGGG":n=$(3,H.Wide);break;case"GGGGG":n=$(3,H.Narrow);break;case"y":n=ue(0,1,0,!1,!0);break;case"yy":n=ue(0,2,0,!0,!0);break;case"yyy":n=ue(0,3,0,!1,!0);break;case"yyyy":n=ue(0,4,0,!1,!0);break;case"Y":n=Ps(1);break;case"YY":n=Ps(2,!0);break;case"YYY":n=Ps(3);break;case"YYYY":n=Ps(4);break;case"M":case"L":n=ue(1,1,1);break;case"MM":case"LL":n=ue(1,2,1);break;case"MMM":n=$(2,H.Abbreviated);break;case"MMMM":n=$(2,H.Wide);break;case"MMMMM":n=$(2,H.Narrow);break;case"LLL":n=$(2,H.Abbreviated,Ie.Standalone);break;case"LLLL":n=$(2,H.Wide,Ie.Standalone);break;case"LLLLL":n=$(2,H.Narrow,Ie.Standalone);break;case"w":n=Md(1);break;case"ww":n=Md(2);break;case"W":n=Md(1,!0);break;case"d":n=ue(2,1);break;case"dd":n=ue(2,2);break;case"c":case"cc":n=ue(7,1);break;case"ccc":n=$(1,H.Abbreviated,Ie.Standalone);break;case"cccc":n=$(1,H.Wide,Ie.Standalone);break;case"ccccc":n=$(1,H.Narrow,Ie.Standalone);break;case"cccccc":n=$(1,H.Short,Ie.Standalone);break;case"E":case"EE":case"EEE":n=$(1,H.Abbreviated);break;case"EEEE":n=$(1,H.Wide);break;case"EEEEE":n=$(1,H.Narrow);break;case"EEEEEE":n=$(1,H.Short);break;case"a":case"aa":case"aaa":n=$(0,H.Abbreviated);break;case"aaaa":n=$(0,H.Wide);break;case"aaaaa":n=$(0,H.Narrow);break;case"b":case"bb":case"bbb":n=$(0,H.Abbreviated,Ie.Standalone,!0);break;case"bbbb":n=$(0,H.Wide,Ie.Standalone,!0);break;case"bbbbb":n=$(0,H.Narrow,Ie.Standalone,!0);break;case"B":case"BB":case"BBB":n=$(0,H.Abbreviated,Ie.Format,!0);break;case"BBBB":n=$(0,H.Wide,Ie.Format,!0);break;case"BBBBB":n=$(0,H.Narrow,Ie.Format,!0);break;case"h":n=ue(3,1,-12);break;case"hh":n=ue(3,2,-12);break;case"H":n=ue(3,1);break;case"HH":n=ue(3,2);break;case"m":n=ue(4,1);break;case"mm":n=ue(4,2);break;case"s":n=ue(5,1);break;case"ss":n=ue(5,2);break;case"S":n=ue(6,1);break;case"SS":n=ue(6,2);break;case"SSS":n=ue(6,3);break;case"Z":case"ZZ":case"ZZZ":n=Fs(0);break;case"ZZZZZ":n=Fs(3);break;case"O":case"OO":case"OOO":case"z":case"zz":case"zzz":n=Fs(1);break;case"OOOO":case"ZZZZ":case"zzzz":n=Fs(2);break;default:return null}return Sd[e]=n,n}function jm(e,n){e=e.replace(/:/g,"");let t=Date.parse("Jan 01, 1970 00:00:00 "+e)/6e4;return isNaN(t)?n:t}function pC(e,n){return e=new Date(e.getTime()),e.setMinutes(e.getMinutes()+n),e}function hC(e,n,t){let i=e.getTimezoneOffset(),o=jm(n,i);return pC(e,-1*(o-i))}function mC(e){if(Tm(e))return e;if(typeof e=="number"&&!isNaN(e))return new Date(e);if(typeof e=="string"){if(e=e.trim(),/^(\d{4}(-\d{1,2}(-\d{1,2})?)?)$/.test(e)){let[i,o=1,s=1]=e.split("-").map(a=>+a);return js(i,o-1,s)}let t=parseFloat(e);if(!isNaN(e-t))return new Date(t);let r;if(r=e.match(rC))return gC(r)}let n=new Date(e);if(!Tm(n))throw new b(2311,!1);return n}function gC(e){let n=new Date(0),t=0,r=0,i=e[8]?n.setUTCFullYear:n.setFullYear,o=e[8]?n.setUTCHours:n.setHours;e[9]&&(t=Number(e[9]+e[10]),r=Number(e[9]+e[11])),i.call(n,Number(e[1]),Number(e[2])-1,Number(e[3]));let s=Number(e[4]||0)-t,a=Number(e[5]||0)-r,c=Number(e[6]||0),l=Math.floor(parseFloat("0."+(e[7]||0))*1e3);return o.call(n,s,a,c,l),n}function Tm(e){return e instanceof Date&&!isNaN(e.valueOf())}var xd=(()=>{class e{_viewContainer;_context=new Bs;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(t,r){this._viewContainer=t,this._thenTemplateRef=r}set ngIf(t){this._context.$implicit=this._context.ngIf=t,this._updateView()}set ngIfThen(t){Mm(t,!1),this._thenTemplateRef=t,this._thenViewRef=null,this._updateView()}set ngIfElse(t){Mm(t,!1),this._elseTemplateRef=t,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(t,r){return!0}static \u0275fac=function(r){return new(r||e)(xe(Fe),xe(ve))};static \u0275dir=I({type:e,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return e})(),Bs=class{$implicit=null;ngIf=null};function Mm(e,n){if(e&&!e.createEmbeddedView)throw new b(2020,!1)}var Rd=(()=>{class e{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=f(le);constructor(t){this._viewContainerRef=t}ngOnChanges(t){if(this._shouldRecreateView(t)){let r=this._viewContainerRef;if(this._viewRef&&r.remove(r.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let i=this._createContextForwardProxy();this._viewRef=r.createEmbeddedView(this.ngTemplateOutlet,i,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(t){return!!t.ngTemplateOutlet||!!t.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(t,r,i)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,r,i):!1,get:(t,r,i)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,r,i)}})}static \u0275fac=function(r){return new(r||e)(xe(Fe))};static \u0275dir=I({type:e,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[jt]})}return e})();function yC(e,n){return new b(2100,!1)}var vC="mediumDate",Bm=new g(""),Vm=new g(""),Ad=(()=>{class e{locale;defaultTimezone;defaultOptions;constructor(t,r,i){this.locale=t,this.defaultTimezone=r,this.defaultOptions=i}transform(t,r,i,o){if(t==null||t===""||t!==t)return null;try{let s=r??this.defaultOptions?.dateFormat??vC,a=i??this.defaultOptions?.timezone??this.defaultTimezone??void 0;return Pm(t,s,o||this.locale,a)}catch(s){throw yC(e,s.message)}}static \u0275fac=function(r){return new(r||e)(xe(Ci,16),xe(Bm,24),xe(Vm,24))};static \u0275pipe=dd({name:"date",type:e,pure:!0})}return e})();var Vs=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Q({type:e});static \u0275inj=G({})}return e})();function Ri(e,n){n=encodeURIComponent(n);for(let t of e.split(";")){let r=t.indexOf("="),[i,o]=r==-1?[t,""]:[t.slice(0,r),t.slice(r+1)];if(i.trim()===n)return decodeURIComponent(o)}return null}var Gn=class{};var Nd="browser";function Hm(e){return e===Nd}var Ai=class{_doc;constructor(n){this._doc=n}manager},Hs=(()=>{class e extends Ai{constructor(t){super(t)}supports(t){return!0}addEventListener(t,r,i,o){return t.addEventListener(r,i,o),()=>this.removeEventListener(t,r,i,o)}removeEventListener(t,r,i,o){return t.removeEventListener(r,i,o)}static \u0275fac=function(r){return new(r||e)(x(L))};static \u0275prov=_({token:e,factory:e.\u0275fac})}return e})(),zs=new g(""),Pd=(()=>{class e{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,r){this._zone=r,t.forEach(s=>{s.manager=this});let i=t.filter(s=>!(s instanceof Hs));this._plugins=i.slice().reverse();let o=t.find(s=>s instanceof Hs);o&&this._plugins.push(o)}addEventListener(t,r,i,o){return this._findPluginFor(r).addEventListener(t,r,i,o)}getZone(){return this._zone}_findPluginFor(t){let r=this._eventNameToPlugin.get(t);if(r)return r;if(r=this._plugins.find(o=>o.supports(t)),!r)throw new b(5101,!1);return this._eventNameToPlugin.set(t,r),r}static \u0275fac=function(r){return new(r||e)(x(zs),x(V))};static \u0275prov=_({token:e,factory:e.\u0275fac})}return e})(),kd="ng-app-id";function Um(e){for(let n of e)n.remove()}function $m(e,n){let t=n.createElement("style");return t.textContent=e,t}function DC(e,n,t,r){let i=e.head?.querySelectorAll(`style[${kd}="${n}"],link[${kd}="${n}"]`);if(i)for(let o of i)o.removeAttribute(kd),o instanceof HTMLLinkElement?r.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&t.set(o.textContent,{usage:0,elements:[o]})}function Fd(e,n){let t=n.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",e),t}var Ld=(()=>{class e{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,r,i,o={}){this.doc=t,this.appId=r,this.nonce=i,DC(t,r,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,r){for(let i of t)this.addUsage(i,this.inline,$m);r?.forEach(i=>this.addUsage(i,this.external,Fd))}removeStyles(t,r){for(let i of t)this.removeUsage(i,this.inline);r?.forEach(i=>this.removeUsage(i,this.external))}addUsage(t,r,i){let o=r.get(t);o?o.usage++:r.set(t,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,i(t,this.doc)))})}removeUsage(t,r){let i=r.get(t);i&&(i.usage--,i.usage<=0&&(Um(i.elements),r.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])Um(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[r,{elements:i}]of this.inline)i.push(this.addElement(t,$m(r,this.doc)));for(let[r,{elements:i}]of this.external)i.push(this.addElement(t,Fd(r,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,r){return this.nonce&&r.setAttribute("nonce",this.nonce),t.appendChild(r)}static \u0275fac=function(r){return new(r||e)(x(L),x(Rr),x(Bn,8),x(jn))};static \u0275prov=_({token:e,factory:e.\u0275fac})}return e})(),Od={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},jd=/%COMP%/g;var Gm="%COMP%",wC=`_nghost-${Gm}`,CC=`_ngcontent-${Gm}`,EC=!0,IC=new g("",{factory:()=>EC});function TC(e){return CC.replace(jd,e)}function MC(e){return wC.replace(jd,e)}function Wm(e,n){return n.map(t=>t.replace(jd,e))}var Bd=(()=>{class e{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,r,i,o,s,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=r,this.appId=i,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Ni(t,s,a,this.tracingService)}createRenderer(t,r){if(!t||!r)return this.defaultRenderer;let i=this.getOrCreateRenderer(t,r);return i instanceof $s?i.applyToHost(t):i instanceof ki&&i.applyStyles(),i}getOrCreateRenderer(t,r){let i=this.rendererByCompId,o=i.get(r.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(r.encapsulation){case at.Emulated:o=new $s(c,l,r,this.appId,d,s,a,u);break;case at.ShadowDom:return new Us(c,t,r,s,a,this.nonce,u,l);case at.ExperimentalIsolatedShadowDom:return new Us(c,t,r,s,a,this.nonce,u);default:o=new ki(c,l,r,d,s,a,u);break}i.set(r.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(r){return new(r||e)(x(Pd),x(Ld),x(Rr),x(IC),x(L),x(V),x(Bn),x(wt,8))};static \u0275prov=_({token:e,factory:e.\u0275fac})}return e})(),Ni=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,t,r,i){this.eventManager=n,this.doc=t,this.ngZone=r,this.tracingService=i}destroy(){}destroyNode=null;createElement(n,t){return t?this.doc.createElementNS(Od[t]||t,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,t){(zm(n)?n.content:n).appendChild(t)}insertBefore(n,t,r){n&&(zm(n)?n.content:n).insertBefore(t,r)}removeChild(n,t){t.remove()}selectRootElement(n,t){let r=typeof n=="string"?this.doc.querySelector(n):n;if(!r)throw new b(-5104,!1);return t||(r.textContent=""),r}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,t,r,i){if(i){t=i+":"+t;let o=Od[i];o?n.setAttributeNS(o,t,r):n.setAttribute(t,r)}else n.setAttribute(t,r)}removeAttribute(n,t,r){if(r){let i=Od[r];i?n.removeAttributeNS(i,t):n.removeAttribute(`${r}:${t}`)}else n.removeAttribute(t)}addClass(n,t){n.classList.add(t)}removeClass(n,t){n.classList.remove(t)}setStyle(n,t,r,i){i&(Dt.DashCase|Dt.Important)?n.style.setProperty(t,r,i&Dt.Important?"important":""):n.style[t]=r}removeStyle(n,t,r){r&Dt.DashCase?n.style.removeProperty(t):n.style[t]=""}setProperty(n,t,r){n!=null&&(n[t]=r)}setValue(n,t){n.nodeValue=t}listen(n,t,r,i){if(typeof n=="string"&&(n=mn().getGlobalEventTarget(this.doc,n),!n))throw new b(5102,!1);let o=this.decoratePreventDefault(r);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,t,o)),this.eventManager.addEventListener(n,t,o,i)}decoratePreventDefault(n){return t=>{if(t==="__ngUnwrap__")return n;n(t)===!1&&t.preventDefault()}}};function zm(e){return e.tagName==="TEMPLATE"&&e.content!==void 0}var Us=class extends Ni{hostEl;sharedStylesHost;shadowRoot;constructor(n,t,r,i,o,s,a,c){super(n,i,o,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=r.styles;l=Wm(r.id,l);for(let u of l){let h=document.createElement("style");s&&h.setAttribute("nonce",s),h.textContent=u,this.shadowRoot.appendChild(h)}let d=r.getExternalStyles?.();if(d)for(let u of d){let h=Fd(u,i);s&&h.setAttribute("nonce",s),this.shadowRoot.appendChild(h)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,t){return super.appendChild(this.nodeOrShadowRoot(n),t)}insertBefore(n,t,r){return super.insertBefore(this.nodeOrShadowRoot(n),t,r)}removeChild(n,t){return super.removeChild(null,t)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},ki=class extends Ni{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,t,r,i,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=i;let l=r.styles;this.styles=c?Wm(c,l):l,this.styleUrls=r.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Tr.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},$s=class extends ki{contentAttr;hostAttr;constructor(n,t,r,i,o,s,a,c){let l=i+"-"+r.id;super(n,t,r,o,s,a,c,l),this.contentAttr=TC(l),this.hostAttr=MC(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,t){let r=super.createElement(n,t);return super.setAttribute(r,this.contentAttr,""),r}};var Gs=class e extends Ii{supportsDOMEvents=!0;static makeCurrent(){Id(new e)}onAndCancel(n,t,r,i){return n.addEventListener(t,r,i),()=>{n.removeEventListener(t,r,i)}}dispatchEvent(n,t){n.dispatchEvent(t)}remove(n){n.remove()}createElement(n,t){return t=t||this.getDefaultDocument(),t.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,t){return t==="window"?window:t==="document"?n:t==="body"?n.body:null}getBaseHref(n){let t=SC();return t==null?null:xC(t)}resetBaseElement(){Oi=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Ri(document.cookie,n)}},Oi=null;function SC(){return Oi=Oi||document.head.querySelector("base"),Oi?Oi.getAttribute("href"):null}function xC(e){return new URL(e,document.baseURI).pathname}var RC=(()=>{class e{build(){return new XMLHttpRequest}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac})}return e})(),qm=["alt","control","meta","shift"],AC={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},NC={alt:e=>e.altKey,control:e=>e.ctrlKey,meta:e=>e.metaKey,shift:e=>e.shiftKey},Zm=(()=>{class e extends Ai{constructor(t){super(t)}supports(t){return e.parseEventName(t)!=null}addEventListener(t,r,i,o){let s=e.parseEventName(r),a=e.eventCallback(s.fullKey,i,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>mn().onAndCancel(t,s.domEventName,a,o))}static parseEventName(t){let r=t.toLowerCase().split("."),i=r.shift();if(r.length===0||!(i==="keydown"||i==="keyup"))return null;let o=e._normalizeKey(r.pop()),s="",a=r.indexOf("code");if(a>-1&&(r.splice(a,1),s="code."),qm.forEach(l=>{let d=r.indexOf(l);d>-1&&(r.splice(d,1),s+=l+".")}),s+=o,r.length!=0||o.length===0)return null;let c={};return c.domEventName=i,c.fullKey=s,c}static matchEventFullKeyCode(t,r){let i=AC[t.key]||t.key,o="";return r.indexOf("code.")>-1&&(i=t.code,o="code."),i==null||!i?!1:(i=i.toLowerCase(),i===" "?i="space":i==="."&&(i="dot"),qm.forEach(s=>{if(s!==i){let a=NC[s];a(t)&&(o+=s+".")}}),o+=i,o===r)}static eventCallback(t,r,i){return o=>{e.matchEventFullKeyCode(o,t)&&i.runGuarded(()=>r(o))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(r){return new(r||e)(x(L))};static \u0275prov=_({token:e,factory:e.\u0275fac})}return e})();async function Vd(e,n,t){let r=q({rootComponent:e},kC(n,t));return wm(r)}function kC(e,n){return{platformRef:n?.platformRef,appProviders:[...jC,...e?.providers??[]],platformProviders:LC}}function OC(){Gs.makeCurrent()}function FC(){return new mt}function PC(){return jl(document),document}var LC=[{provide:jn,useValue:Nd},{provide:vs,useValue:OC,multi:!0},{provide:L,useFactory:PC}];var jC=[{provide:ri,useValue:"root"},{provide:mt,useFactory:FC},{provide:zs,useClass:Hs,multi:!0},{provide:zs,useClass:Zm,multi:!0},Bd,Ld,Pd,{provide:He,useExisting:Bd},{provide:Gn,useClass:RC},[]];var St=class e{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(t=>{let r=t.indexOf(":");if(r>0){let i=t.slice(0,r),o=t.slice(r+1).trim();this.addHeaderEntry(i,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((t,r)=>{this.addHeaderEntry(r,t)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([t,r])=>{this.setHeaderEntries(t,r)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let t=this.headers.get(n.toLowerCase());return t&&t.length>0?t[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,t){return this.clone({name:n,value:t,op:"a"})}set(n,t){return this.clone({name:n,value:t,op:"s"})}delete(n,t){return this.clone({name:n,value:t,op:"d"})}maybeSetNormalizedName(n,t){this.normalizedNames.has(t)||this.normalizedNames.set(t,n)}init(){this.lazyInit&&(this.lazyInit instanceof e?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(t=>{this.headers.set(t,n.headers.get(t)),this.normalizedNames.set(t,n.normalizedNames.get(t))})}clone(n){let t=new e;return t.lazyInit=this.lazyInit&&this.lazyInit instanceof e?this.lazyInit:this,t.lazyUpdate=(this.lazyUpdate||[]).concat([n]),t}applyUpdate(n){let t=n.name.toLowerCase();switch(n.op){case"a":case"s":let r=n.value;if(typeof r=="string"&&(r=[r]),r.length===0)return;this.maybeSetNormalizedName(n.name,t);let i=(n.op==="a"?this.headers.get(t):void 0)||[];i.push(...r),this.headers.set(t,i);break;case"d":let o=n.value;if(!o)this.headers.delete(t),this.normalizedNames.delete(t);else{let s=this.headers.get(t);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(t),this.normalizedNames.delete(t)):this.headers.set(t,s)}break}}addHeaderEntry(n,t){let r=n.toLowerCase();this.maybeSetNormalizedName(n,r),this.headers.has(r)?this.headers.get(r).push(t):this.headers.set(r,[t])}setHeaderEntries(n,t){let r=(Array.isArray(t)?t:[t]).map(o=>o.toString()),i=n.toLowerCase();this.headers.set(i,r),this.maybeSetNormalizedName(n,i)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(t=>n(this.normalizedNames.get(t),this.headers.get(t)))}};var Zs=class{map=new Map;set(n,t){return this.map.set(n,t),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Qs=class{encodeKey(n){return Qm(n)}encodeValue(n){return Qm(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function BC(e,n){let t=new Map;return e.length>0&&e.replace(/^\?/,"").split("&").forEach(i=>{let o=i.indexOf("="),[s,a]=o==-1?[n.decodeKey(i),""]:[n.decodeKey(i.slice(0,o)),n.decodeValue(i.slice(o+1))],c=t.get(s)||[];c.push(a),t.set(s,c)}),t}var VC=/%(\d[a-f0-9])/gi,HC={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function Qm(e){return encodeURIComponent(e).replace(VC,(n,t)=>HC[t]??n)}function Ws(e){return`${e}`}var Wt=class e{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Qs,n.fromString){if(n.fromObject)throw new b(2805,!1);this.map=BC(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(t=>{let r=n.fromObject[t],i=Array.isArray(r)?r.map(Ws):[Ws(r)];this.map.set(t,i)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let t=this.map.get(n);return t?t[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,t){return this.clone({param:n,value:t,op:"a"})}appendAll(n){let t=[];return Object.keys(n).forEach(r=>{let i=n[r];Array.isArray(i)?i.forEach(o=>{t.push({param:r,value:o,op:"a"})}):t.push({param:r,value:i,op:"a"})}),this.clone(t)}set(n,t){return this.clone({param:n,value:t,op:"s"})}delete(n,t){return this.clone({param:n,value:t,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let t=this.encoder.encodeKey(n);return this.map.get(n).map(r=>t+"="+this.encoder.encodeValue(r)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let t=new e({encoder:this.encoder});return t.cloneFrom=this.cloneFrom||this,t.updates=(this.updates||[]).concat(n),t}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let t=(n.op==="a"?this.map.get(n.param):void 0)||[];t.push(Ws(n.value)),this.map.set(n.param,t);break;case"d":if(n.value!==void 0){let r=this.map.get(n.param)||[],i=r.indexOf(Ws(n.value));i!==-1&&r.splice(i,1),r.length>0?this.map.set(n.param,r):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function UC(e){switch(e){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function Ym(e){return typeof ArrayBuffer<"u"&&e instanceof ArrayBuffer}function Km(e){return typeof Blob<"u"&&e instanceof Blob}function Xm(e){return typeof FormData<"u"&&e instanceof FormData}function $C(e){return typeof URLSearchParams<"u"&&e instanceof URLSearchParams}var Fi="Content-Type",Ys="Accept",Jm="text/plain",eg="application/json",tg=`${eg}, ${Jm}, */*`,Fr=class e{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,t,r,i){this.url=t,this.method=n.toUpperCase();let o;if(UC(this.method)||i?(this.body=r!==void 0?r:null,o=i):o=r,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new b(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new St,this.context??=new Zs,!this.params)this.params=new Wt,this.urlWithParams=t;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=t;else{let a=t.indexOf("?"),c=a===-1?"?":a<t.length-1?"&":"";this.urlWithParams=t+c+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||Ym(this.body)||Km(this.body)||Xm(this.body)||$C(this.body)?this.body:this.body instanceof Wt?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||Xm(this.body)?null:Km(this.body)?this.body.type||null:Ym(this.body)?null:typeof this.body=="string"?Jm:this.body instanceof Wt?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?eg:null}clone(n={}){let t=n.method||this.method,r=n.url||this.url,i=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,d=n.credentials||this.credentials,u=n.referrer??this.referrer,h=n.integrity||this.integrity,p=n.referrerPolicy||this.referrerPolicy,m=n.transferCache??this.transferCache,D=n.timeout??this.timeout,y=n.body!==void 0?n.body:this.body,v=n.withCredentials??this.withCredentials,se=n.reportProgress??this.reportProgress,Xe=n.headers||this.headers,re=n.params||this.params,Zt=n.context??this.context;return n.setHeaders!==void 0&&(Xe=Object.keys(n.setHeaders).reduce((Qt,xt)=>Qt.set(xt,n.setHeaders[xt]),Xe)),n.setParams&&(re=Object.keys(n.setParams).reduce((Qt,xt)=>Qt.set(xt,n.setParams[xt]),re)),new e(t,r,y,{params:re,headers:Xe,context:Zt,reportProgress:se,responseType:i,withCredentials:v,transferCache:m,keepalive:o,cache:a,priority:s,timeout:D,mode:c,redirect:l,credentials:d,referrer:u,integrity:h,referrerPolicy:p})}},qt=(function(e){return e[e.Sent=0]="Sent",e[e.UploadProgress=1]="UploadProgress",e[e.ResponseHeader=2]="ResponseHeader",e[e.DownloadProgress=3]="DownloadProgress",e[e.Response=4]="Response",e[e.User=5]="User",e})(qt||{}),Pr=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,t=200,r="OK"){this.headers=n.headers||new St,this.status=n.status!==void 0?n.status:t,this.statusText=n.statusText||r,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Pi=class e extends Pr{constructor(n={}){super(n)}type=qt.ResponseHeader;clone(n={}){return new e({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Lr=class e extends Pr{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=qt.Response;clone(n={}){return new e({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},Mt=class extends Pr{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},ng=200,zC=204;var GC=/^\)\]\}',?\n/,rg=new g(""),qs=(()=>{class e{fetchImpl=f(Ud,{optional:!0})?.fetch??((...t)=>globalThis.fetch(...t));ngZone=f(V);destroyRef=f(Ft);handle(t){return new N(r=>{let i=new AbortController;this.doRequest(t,i.signal,r).then($d,s=>r.error(new Mt({error:s})));let o;return t.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{i.signal.aborted||i.abort(new DOMException("signal timed out","TimeoutError"))},t.timeout))),()=>{o!==void 0&&clearTimeout(o),i.abort()}})}async doRequest(t,r,i){let o=this.createRequestInit(t),s;try{let D=this.ngZone.runOutsideAngular(()=>this.fetchImpl(t.urlWithParams,q({signal:r},o)));WC(D),i.next({type:qt.Sent}),s=await D}catch(D){i.error(new Mt({error:D,status:D.status??0,statusText:D.statusText,url:t.urlWithParams,headers:D.headers}));return}let a=new St(s.headers),c=s.statusText,l=s.url||t.urlWithParams,d=s.status,u=null;if(t.reportProgress&&i.next(new Pi({headers:a,status:d,statusText:c,url:l})),s.body){let D=s.headers.get("content-length"),y=[],v=s.body.getReader(),se=0,Xe,re,Zt=typeof Zone<"u"&&Zone.current,Qt=!1;if(await this.ngZone.runOutsideAngular(async()=>{for(;;){if(this.destroyRef.destroyed){await v.cancel(),Qt=!0;break}let{done:Hr,value:va}=await v.read();if(Hr)break;if(y.push(va),se+=va.length,t.reportProgress){re=t.responseType==="text"?(re??"")+(Xe??=new TextDecoder).decode(va,{stream:!0}):void 0;let yu=()=>i.next({type:qt.DownloadProgress,total:D?+D:void 0,loaded:se,partialText:re});Zt?Zt.run(yu):yu()}}}),Qt){i.complete();return}let xt=this.concatChunks(y,se);try{let Hr=s.headers.get(Fi)??"";u=this.parseBody(t,xt,Hr,d)}catch(Hr){i.error(new Mt({error:Hr,headers:new St(s.headers),status:s.status,statusText:s.statusText,url:s.url||t.urlWithParams}));return}}d===0&&(d=u?ng:0);let h=d>=200&&d<300,p=s.redirected,m=s.type;h?(i.next(new Lr({body:u,headers:a,status:d,statusText:c,url:l,redirected:p,responseType:m})),i.complete()):i.error(new Mt({error:u,headers:a,status:d,statusText:c,url:l,redirected:p,responseType:m}))}parseBody(t,r,i,o){switch(t.responseType){case"json":let s=new TextDecoder().decode(r).replace(GC,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(r);case"blob":return new Blob([r],{type:i});case"arraybuffer":return r.buffer}}createRequestInit(t){let r={},i;if(i=t.credentials,t.withCredentials&&(i="include"),t.headers.forEach((o,s)=>r[o]=s.join(",")),t.headers.has(Ys)||(r[Ys]=tg),!t.headers.has(Fi)){let o=t.detectContentTypeHeader();o!==null&&(r[Fi]=o)}return{body:t.serializeBody(),method:t.method,headers:r,credentials:i,keepalive:t.keepalive,cache:t.cache,priority:t.priority,mode:t.mode,redirect:t.redirect,referrer:t.referrer,integrity:t.integrity,referrerPolicy:t.referrerPolicy}}concatChunks(t,r){let i=new Uint8Array(r),o=0;for(let s of t)i.set(s,o),o+=s.length;return i}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac})}return e})(),Ud=class{};function $d(){}function WC(e){e.then($d,$d)}var qC=/^\)\]\}',?\n/;var zd=(()=>{class e{xhrFactory;tracingService=f(wt,{optional:!0});constructor(t){this.xhrFactory=t}maybePropagateTrace(t){return this.tracingService?.propagate?this.tracingService.propagate(t):t}handle(t){if(t.method==="JSONP")throw new b(-2800,!1);let r=this.xhrFactory;return pt(null).pipe(To(()=>new N(o=>{let s=r.build();if(s.open(t.method,t.urlWithParams),t.withCredentials&&(s.withCredentials=!0),t.headers.forEach((y,v)=>s.setRequestHeader(y,v.join(","))),t.headers.has(Ys)||s.setRequestHeader(Ys,tg),!t.headers.has(Fi)){let y=t.detectContentTypeHeader();y!==null&&s.setRequestHeader(Fi,y)}if(t.timeout&&(s.timeout=t.timeout),t.responseType){let y=t.responseType.toLowerCase();s.responseType=y!=="json"?y:"text"}let a=t.serializeBody(),c=null,l=()=>{if(c!==null)return c;let y=s.statusText||"OK",v=new St(s.getAllResponseHeaders()),se=s.responseURL||t.url;return c=new Pi({headers:v,status:s.status,statusText:y,url:se}),c},d=this.maybePropagateTrace(()=>{let{headers:y,status:v,statusText:se,url:Xe}=l(),re=null;v!==zC&&(re=typeof s.response>"u"?s.responseText:s.response),v===0&&(v=re?ng:0);let Zt=v>=200&&v<300;if(t.responseType==="json"&&typeof re=="string"){let Qt=re;re=re.replace(qC,"");try{re=re!==""?JSON.parse(re):null}catch(xt){re=Qt,Zt&&(Zt=!1,re={error:xt,text:re})}}Zt?(o.next(new Lr({body:re,headers:y,status:v,statusText:se,url:Xe||void 0})),o.complete()):o.error(new Mt({error:re,headers:y,status:v,statusText:se,url:Xe||void 0}))}),u=this.maybePropagateTrace(y=>{let{url:v}=l(),se=new Mt({error:y,status:s.status||0,statusText:s.statusText||"Unknown Error",url:v||void 0});o.error(se)}),h=u;t.timeout&&(h=this.maybePropagateTrace(y=>{let{url:v}=l(),se=new Mt({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:v||void 0});o.error(se)}));let p=!1,m=this.maybePropagateTrace(y=>{p||(o.next(l()),p=!0);let v={type:qt.DownloadProgress,loaded:y.loaded};y.lengthComputable&&(v.total=y.total),t.responseType==="text"&&s.responseText&&(v.partialText=s.responseText),o.next(v)}),D=this.maybePropagateTrace(y=>{let v={type:qt.UploadProgress,loaded:y.loaded};y.lengthComputable&&(v.total=y.total),o.next(v)});return s.addEventListener("load",d),s.addEventListener("error",u),s.addEventListener("timeout",h),s.addEventListener("abort",u),t.reportProgress&&(s.addEventListener("progress",m),a!==null&&s.upload&&s.upload.addEventListener("progress",D)),s.send(a),o.next({type:qt.Sent}),()=>{s.removeEventListener("error",u),s.removeEventListener("abort",u),s.removeEventListener("load",d),s.removeEventListener("timeout",h),t.reportProgress&&(s.removeEventListener("progress",m),a!==null&&s.upload&&s.upload.removeEventListener("progress",D)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(r){return new(r||e)(x(Gn))};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function ZC(e,n){return n(e)}function QC(e,n,t){return(r,i)=>ur(t,()=>n(r,o=>e(o,i)))}var ig=new g("",{factory:()=>[]}),og=new g(""),sg=new g("",{factory:()=>!0});var Xs=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:function(r){let i=null;return r?i=new(r||e):i=x(zd),i},providedIn:"root"})}return e})();var Ks=(()=>{class e{backend;injector;chain=null;pendingTasks=f(Jo);contributeToStability=f(sg);constructor(t,r){this.backend=t,this.injector=r}handle(t){if(this.chain===null){let r=Array.from(new Set([...this.injector.get(ig),...this.injector.get(og,[])]));this.chain=r.reduceRight((i,o)=>QC(i,o,this.injector),ZC)}if(this.contributeToStability){let r=this.pendingTasks.add();return this.chain(t,i=>this.backend.handle(i)).pipe($a(r))}else return this.chain(t,r=>this.backend.handle(r))}static \u0275fac=function(r){return new(r||e)(x(Xs),x(ye))};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Gd=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:function(r){let i=null;return r?i=new(r||e):i=x(Ks),i},providedIn:"root"})}return e})();function Hd(e,n){return{body:n,headers:e.headers,context:e.context,observe:e.observe,params:e.params,reportProgress:e.reportProgress,responseType:e.responseType,withCredentials:e.withCredentials,credentials:e.credentials,transferCache:e.transferCache,timeout:e.timeout,keepalive:e.keepalive,priority:e.priority,cache:e.cache,mode:e.mode,redirect:e.redirect,integrity:e.integrity,referrer:e.referrer,referrerPolicy:e.referrerPolicy}}var Js=(()=>{class e{handler;constructor(t){this.handler=t}request(t,r,i={}){let o;if(t instanceof Fr)o=t;else{let c;i.headers instanceof St?c=i.headers:c=new St(i.headers);let l;i.params&&(i.params instanceof Wt?l=i.params:l=new Wt({fromObject:i.params})),o=new Fr(t,r,i.body!==void 0?i.body:null,{headers:c,context:i.context,params:l,reportProgress:i.reportProgress,responseType:i.responseType||"json",withCredentials:i.withCredentials,transferCache:i.transferCache,keepalive:i.keepalive,priority:i.priority,cache:i.cache,mode:i.mode,redirect:i.redirect,credentials:i.credentials,referrer:i.referrer,referrerPolicy:i.referrerPolicy,integrity:i.integrity,timeout:i.timeout})}let s=pt(o).pipe(Ua(c=>this.handler.handle(c)));if(t instanceof Fr||i.observe==="events")return s;let a=s.pipe(rr(c=>c instanceof Lr));switch(i.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(ze(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new b(2806,!1);return c.body}));case"blob":return a.pipe(ze(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new b(2807,!1);return c.body}));case"text":return a.pipe(ze(c=>{if(c.body!==null&&typeof c.body!="string")throw new b(2808,!1);return c.body}));default:return a.pipe(ze(c=>c.body))}case"response":return a;default:throw new b(2809,!1)}}delete(t,r={}){return this.request("DELETE",t,r)}get(t,r={}){return this.request("GET",t,r)}head(t,r={}){return this.request("HEAD",t,r)}jsonp(t,r){return this.request("JSONP",t,{params:new Wt().append(r,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(t,r={}){return this.request("OPTIONS",t,r)}patch(t,r,i={}){return this.request("PATCH",t,Hd(i,r))}post(t,r,i={}){return this.request("POST",t,Hd(i,r))}put(t,r,i={}){return this.request("PUT",t,Hd(i,r))}static \u0275fac=function(r){return new(r||e)(x(Gd))};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var YC=new g("",{factory:()=>!0}),KC="XSRF-TOKEN",XC=new g("",{factory:()=>KC}),JC="X-XSRF-TOKEN",eE=new g("",{factory:()=>JC}),tE=(()=>{class e{cookieName=f(XC);doc=f(L);lastCookieString="";lastToken=null;parseCount=0;getToken(){let t=this.doc.cookie||"";return t!==this.lastCookieString&&(this.parseCount++,this.lastToken=Ri(t,this.cookieName),this.lastCookieString=t),this.lastToken}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),ag=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:function(r){let i=null;return r?i=new(r||e):i=x(tE),i},providedIn:"root"})}return e})();function nE(e,n){if(!f(YC)||e.method==="GET"||e.method==="HEAD")return n(e);try{let i=f(Ns).href,{origin:o}=new URL(i),{origin:s}=new URL(e.url,o);if(o!==s)return n(e)}catch{return n(e)}let t=f(ag).getToken(),r=f(eE);return t!=null&&!e.headers.has(r)&&(e=e.clone({headers:e.headers.set(r,t)})),n(e)}var Wd=(function(e){return e[e.Interceptors=0]="Interceptors",e[e.LegacyInterceptors=1]="LegacyInterceptors",e[e.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",e[e.NoXsrfProtection=3]="NoXsrfProtection",e[e.JsonpSupport=4]="JsonpSupport",e[e.RequestsMadeViaParent=5]="RequestsMadeViaParent",e[e.Fetch=6]="Fetch",e})(Wd||{});function rE(e,n){return{\u0275kind:e,\u0275providers:n}}function qd(...e){let n=[Js,Ks,{provide:Gd,useExisting:Ks},{provide:Xs,useFactory:()=>f(rg,{optional:!0})??f(zd)},{provide:ig,useValue:nE,multi:!0}];for(let t of e)n.push(...t.\u0275providers);return lr(n)}function Zd(){return rE(Wd.Fetch,[qs,{provide:rg,useExisting:qs},{provide:Xs,useExisting:qs}])}var cg={providers:[Yc(),qd(Zd())]};var oE=new g("cdk-dir-doc",{providedIn:"root",factory:()=>f(L)}),sE=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function aE(e){let n=e?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?sE.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Qd=(()=>{class e{get value(){return this.valueSignal()}valueSignal=ci("ltr");change=new ge;constructor(){let t=f(oE,{optional:!0});if(t){let r=t.body?t.body.dir:null,i=t.documentElement?t.documentElement.dir:null;this.valueSignal.set(aE(r||i||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Yd;try{Yd=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Yd=!1}var $e=(()=>{class e{_platformId=f(jn);isBrowser=this._platformId?Hm(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Yd)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function gn(e){return e instanceof U?e.nativeElement:e}function ea(e){return e&&typeof e.connect=="function"&&!(e instanceof $r)}var lt=(function(e){return e[e.REPLACED=0]="REPLACED",e[e.INSERTED=1]="INSERTED",e[e.MOVED=2]="MOVED",e[e.REMOVED=3]="REMOVED",e})(lt||{}),ta=class{viewCacheSize=20;_viewCache=[];applyChanges(n,t,r,i,o){n.forEachOperation((s,a,c)=>{let l,d;if(s.previousIndex==null){let u=()=>r(s,a,c);l=this._insertView(u,c,t,i(s)),d=l?lt.INSERTED:lt.REPLACED}else c==null?(this._detachAndCacheView(a,t),d=lt.REMOVED):(l=this._moveView(a,c,t,i(s)),d=lt.MOVED);o&&o({context:l?.context,operation:d,record:s})})}detach(){for(let n of this._viewCache)n.destroy();this._viewCache=[]}_insertView(n,t,r,i){let o=this._insertViewFromCache(t,r);if(o){o.context.$implicit=i;return}let s=n();return r.createEmbeddedView(s.templateRef,s.context,s.index)}_detachAndCacheView(n,t){let r=t.detach(n);this._maybeCacheView(r,t)}_moveView(n,t,r,i){let o=r.get(n);return r.move(o,t),o.context.$implicit=i,o}_maybeCacheView(n,t){if(this._viewCache.length<this.viewCacheSize)this._viewCache.push(n);else{let r=t.indexOf(n);r===-1?n.destroy():t.remove(r)}}_insertViewFromCache(n,t){let r=this._viewCache.pop();return r&&t.insert(r,n),r||null}};var Ae=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Q({type:e});static \u0275inj=G({})}return e})();var cE=20,dg=(()=>{class e{_platform=f($e);_listeners;_viewportSize=null;_change=new J;_document=f(L);constructor(){let t=f(V),r=f(He).createRenderer(null,null);t.runOutsideAngular(()=>{if(this._platform.isBrowser){let i=o=>this._change.next(o);this._listeners=[r.listen("window","resize",i),r.listen("window","orientationchange",i)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(t=>t()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let t={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),t}getViewportRect(){let t=this.getViewportScrollPosition(),{width:r,height:i}=this.getViewportSize();return{top:t.top,left:t.left,bottom:t.top+i,right:t.left+r,height:i,width:r}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let t=this._document,r=this._getWindow(),i=t.documentElement,o=i.getBoundingClientRect(),s=-o.top||t.body?.scrollTop||r.scrollY||i.scrollTop||0,a=-o.left||t.body?.scrollLeft||r.scrollX||i.scrollLeft||0;return{top:s,left:a}}change(t=cE){return t>0?this._change.pipe(qr(t)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let t=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:t.innerWidth,height:t.innerHeight}:{width:0,height:0}}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var ug=new g("CDK_VIRTUAL_SCROLL_VIEWPORT");var lg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Q({type:e});static \u0275inj=G({})}return e})(),fg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Q({type:e});static \u0275inj=G({imports:[Ae,lg,Ae,lg]})}return e})();var na=class{applyChanges(n,t,r,i,o){n.forEachOperation((s,a,c)=>{let l,d;if(s.previousIndex==null){let u=r(s,a,c);l=t.createEmbeddedView(u.templateRef,u.context,u.index),d=lt.INSERTED}else c==null?(t.remove(a),d=lt.REMOVED):(l=t.get(a),t.move(l,c),d=lt.MOVED);o&&o({context:l?.context,operation:d,record:s})})}detach(){}};var lE=[[["caption"]],[["colgroup"],["col"]],"*"],dE=["caption","colgroup, col","*"];function uE(e,n){e&1&&Y(0,2)}function fE(e,n){e&1&&(E(0,"thead",0),de(1,1),T(),E(2,"tbody",0),de(3,2)(4,3),T(),E(5,"tfoot",0),de(6,4),T())}function pE(e,n){e&1&&de(0,1)(1,2)(2,3)(3,4)}var dt=new g("CDK_TABLE");var oa=(()=>{class e{template=f(ve);constructor(){}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","cdkCellDef",""]]})}return e})(),sa=(()=>{class e{template=f(ve);constructor(){}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","cdkHeaderCellDef",""]]})}return e})(),mg=(()=>{class e{template=f(ve);constructor(){}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","cdkFooterCellDef",""]]})}return e})(),jr=(()=>{class e{_table=f(dt,{optional:!0});_hasStickyChanged=!1;get name(){return this._name}set name(t){this._setNameInput(t)}_name;get sticky(){return this._sticky}set sticky(t){t!==this._sticky&&(this._sticky=t,this._hasStickyChanged=!0)}_sticky=!1;get stickyEnd(){return this._stickyEnd}set stickyEnd(t){t!==this._stickyEnd&&(this._stickyEnd=t,this._hasStickyChanged=!0)}_stickyEnd=!1;cell;headerCell;footerCell;cssClassFriendlyName;_columnCssClassName;constructor(){}hasStickyChanged(){let t=this._hasStickyChanged;return this.resetStickyChanged(),t}resetStickyChanged(){this._hasStickyChanged=!1}_updateColumnCssClassName(){this._columnCssClassName=[`cdk-column-${this.cssClassFriendlyName}`]}_setNameInput(t){t&&(this._name=t,this.cssClassFriendlyName=t.replace(/[^a-z0-9_-]/gi,"-"),this._updateColumnCssClassName())}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","cdkColumnDef",""]],contentQueries:function(r,i,o){if(r&1&&$n(o,oa,5)(o,sa,5)(o,mg,5),r&2){let s;te(s=ne())&&(i.cell=s.first),te(s=ne())&&(i.headerCell=s.first),te(s=ne())&&(i.footerCell=s.first)}},inputs:{name:[0,"cdkColumnDef","name"],sticky:[2,"sticky","sticky",oe],stickyEnd:[2,"stickyEnd","stickyEnd",oe]}})}return e})(),ia=class{constructor(n,t){t.nativeElement.classList.add(...n._columnCssClassName)}},gg=(()=>{class e extends ia{constructor(){super(f(jr),f(U))}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["cdk-header-cell"],["th","cdk-header-cell",""]],hostAttrs:["role","columnheader",1,"cdk-header-cell"],features:[W]})}return e})();var yg=(()=>{class e extends ia{constructor(){let t=f(jr),r=f(U);super(t,r);let i=t._table?._getCellRole();i&&r.nativeElement.setAttribute("role",i)}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["cdk-cell"],["td","cdk-cell",""]],hostAttrs:[1,"cdk-cell"],features:[W]})}return e})();var Xd=(()=>{class e{template=f(ve);_differs=f($t);columns;_columnsDiffer;constructor(){}ngOnChanges(t){if(!this._columnsDiffer){let r=t.columns&&t.columns.currentValue||[];this._columnsDiffer=this._differs.find(r).create(),this._columnsDiffer.diff(r)}}getColumnsDiff(){return this._columnsDiffer.diff(this.columns)}extractCellTemplate(t){return this instanceof ji?t.headerCell.template:this instanceof Jd?t.footerCell.template:t.cell.template}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,features:[jt]})}return e})(),ji=(()=>{class e extends Xd{_table=f(dt,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(t){t!==this._sticky&&(this._sticky=t,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(f(ve),f($t))}ngOnChanges(t){super.ngOnChanges(t)}hasStickyChanged(){let t=this._hasStickyChanged;return this.resetStickyChanged(),t}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","cdkHeaderRowDef",""]],inputs:{columns:[0,"cdkHeaderRowDef","columns"],sticky:[2,"cdkHeaderRowDefSticky","sticky",oe]},features:[W,jt]})}return e})(),Jd=(()=>{class e extends Xd{_table=f(dt,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(t){t!==this._sticky&&(this._sticky=t,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(f(ve),f($t))}ngOnChanges(t){super.ngOnChanges(t)}hasStickyChanged(){let t=this._hasStickyChanged;return this.resetStickyChanged(),t}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","cdkFooterRowDef",""]],inputs:{columns:[0,"cdkFooterRowDef","columns"],sticky:[2,"cdkFooterRowDefSticky","sticky",oe]},features:[W,jt]})}return e})(),aa=(()=>{class e extends Xd{_table=f(dt,{optional:!0});when;constructor(){super(f(ve),f($t))}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","cdkRowDef",""]],inputs:{columns:[0,"cdkRowDefColumns","columns"],when:[0,"cdkRowDefWhen","when"]},features:[W]})}return e})(),Wn=(()=>{class e{_viewContainer=f(Fe);cells;context;static mostRecentCellOutlet=null;constructor(){e.mostRecentCellOutlet=this}ngOnDestroy(){e.mostRecentCellOutlet===this&&(e.mostRecentCellOutlet=null)}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","cdkCellOutlet",""]]})}return e})(),eu=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=P({type:e,selectors:[["cdk-header-row"],["tr","cdk-header-row",""]],hostAttrs:["role","row",1,"cdk-header-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(r,i){r&1&&de(0,0)},dependencies:[Wn],encapsulation:2})}return e})();var tu=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=P({type:e,selectors:[["cdk-row"],["tr","cdk-row",""]],hostAttrs:["role","row",1,"cdk-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(r,i){r&1&&de(0,0)},dependencies:[Wn],encapsulation:2})}return e})(),vg=(()=>{class e{templateRef=f(ve);_contentClassNames=["cdk-no-data-row","cdk-row"];_cellClassNames=["cdk-cell","cdk-no-data-cell"];_cellSelector="td, cdk-cell, [cdk-cell], .cdk-cell";constructor(){}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["ng-template","cdkNoDataRow",""]]})}return e})(),pg=["top","bottom","left","right"],Kd=class{_isNativeHtmlTable;_stickCellCss;_isBrowser;_needsPositionStickyOnElement;direction;_positionListener;_tableInjector;_elemSizeCache=new WeakMap;_resizeObserver=globalThis?.ResizeObserver?new globalThis.ResizeObserver(n=>this._updateCachedSizes(n)):null;_updatedStickyColumnsParamsToReplay=[];_stickyColumnsReplayTimeout=null;_cachedCellWidths=[];_borderCellCss;_destroyed=!1;constructor(n,t,r=!0,i=!0,o,s,a){this._isNativeHtmlTable=n,this._stickCellCss=t,this._isBrowser=r,this._needsPositionStickyOnElement=i,this.direction=o,this._positionListener=s,this._tableInjector=a,this._borderCellCss={top:`${t}-border-elem-top`,bottom:`${t}-border-elem-bottom`,left:`${t}-border-elem-left`,right:`${t}-border-elem-right`}}clearStickyPositioning(n,t){(t.includes("left")||t.includes("right"))&&this._removeFromStickyColumnReplayQueue(n);let r=[];for(let i of n)i.nodeType===i.ELEMENT_NODE&&r.push(i,...Array.from(i.children));un({write:()=>{for(let i of r)this._removeStickyStyle(i,t)}},{injector:this._tableInjector})}updateStickyColumns(n,t,r,i=!0,o=!0){if(!n.length||!this._isBrowser||!(t.some(y=>y)||r.some(y=>y))){this._positionListener?.stickyColumnsUpdated({sizes:[]}),this._positionListener?.stickyEndColumnsUpdated({sizes:[]});return}let s=n[0],a=s.children.length,c=this.direction==="rtl",l=c?"right":"left",d=c?"left":"right",u=t.lastIndexOf(!0),h=r.indexOf(!0),p,m,D;o&&this._updateStickyColumnReplayQueue({rows:[...n],stickyStartStates:[...t],stickyEndStates:[...r]}),un({earlyRead:()=>{p=this._getCellWidths(s,i),m=this._getStickyStartColumnPositions(p,t),D=this._getStickyEndColumnPositions(p,r)},write:()=>{for(let y of n)for(let v=0;v<a;v++){let se=y.children[v];t[v]&&this._addStickyStyle(se,l,m[v],v===u),r[v]&&this._addStickyStyle(se,d,D[v],v===h)}this._positionListener&&p.some(y=>!!y)&&(this._positionListener.stickyColumnsUpdated({sizes:u===-1?[]:p.slice(0,u+1).map((y,v)=>t[v]?y:null)}),this._positionListener.stickyEndColumnsUpdated({sizes:h===-1?[]:p.slice(h).map((y,v)=>r[v+h]?y:null).reverse()}))}},{injector:this._tableInjector})}stickRows(n,t,r){if(!this._isBrowser)return;let i=r==="bottom"?n.slice().reverse():n,o=r==="bottom"?t.slice().reverse():t,s=[],a=[],c=[];un({earlyRead:()=>{for(let l=0,d=0;l<i.length;l++){if(!o[l])continue;s[l]=d;let u=i[l];c[l]=this._isNativeHtmlTable?Array.from(u.children):[u];let h=this._retrieveElementSize(u).height;d+=h,a[l]=h}},write:()=>{let l=o.lastIndexOf(!0);for(let d=0;d<i.length;d++){if(!o[d])continue;let u=s[d],h=d===l;for(let p of c[d])this._addStickyStyle(p,r,u,h)}r==="top"?this._positionListener?.stickyHeaderRowsUpdated({sizes:a,offsets:s,elements:c}):this._positionListener?.stickyFooterRowsUpdated({sizes:a,offsets:s,elements:c})}},{injector:this._tableInjector})}updateStickyFooterContainer(n,t){this._isNativeHtmlTable&&un({write:()=>{let r=n.querySelector("tfoot");r&&(t.some(i=>!i)?this._removeStickyStyle(r,["bottom"]):this._addStickyStyle(r,"bottom",0,!1))}},{injector:this._tableInjector})}destroy(){this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._resizeObserver?.disconnect(),this._destroyed=!0}_removeStickyStyle(n,t){if(!n.classList.contains(this._stickCellCss))return;for(let i of t)n.style[i]="",n.classList.remove(this._borderCellCss[i]);pg.some(i=>t.indexOf(i)===-1&&n.style[i])?n.style.zIndex=this._getCalculatedZIndex(n):(n.style.zIndex="",this._needsPositionStickyOnElement&&(n.style.position=""),n.classList.remove(this._stickCellCss))}_addStickyStyle(n,t,r,i){n.classList.add(this._stickCellCss),i&&n.classList.add(this._borderCellCss[t]),n.style[t]=`${r}px`,n.style.zIndex=this._getCalculatedZIndex(n),this._needsPositionStickyOnElement&&(n.style.cssText+="position: -webkit-sticky; position: sticky; ")}_getCalculatedZIndex(n){let t={top:100,bottom:10,left:1,right:1},r=0;for(let i of pg)n.style[i]&&(r+=t[i]);return r?`${r}`:""}_getCellWidths(n,t=!0){if(!t&&this._cachedCellWidths.length)return this._cachedCellWidths;let r=[],i=n.children;for(let o=0;o<i.length;o++){let s=i[o];r.push(this._retrieveElementSize(s).width)}return this._cachedCellWidths=r,r}_getStickyStartColumnPositions(n,t){let r=[],i=0;for(let o=0;o<n.length;o++)t[o]&&(r[o]=i,i+=n[o]);return r}_getStickyEndColumnPositions(n,t){let r=[],i=0;for(let o=n.length;o>0;o--)t[o]&&(r[o]=i,i+=n[o]);return r}_retrieveElementSize(n){let t=this._elemSizeCache.get(n);if(t)return t;let r=n.getBoundingClientRect(),i={width:r.width,height:r.height};return this._resizeObserver&&(this._elemSizeCache.set(n,i),this._resizeObserver.observe(n,{box:"border-box"})),i}_updateStickyColumnReplayQueue(n){this._removeFromStickyColumnReplayQueue(n.rows),this._stickyColumnsReplayTimeout||this._updatedStickyColumnsParamsToReplay.push(n)}_removeFromStickyColumnReplayQueue(n){let t=new Set(n);for(let r of this._updatedStickyColumnsParamsToReplay)r.rows=r.rows.filter(i=>!t.has(i));this._updatedStickyColumnsParamsToReplay=this._updatedStickyColumnsParamsToReplay.filter(r=>!!r.rows.length)}_updateCachedSizes(n){let t=!1;for(let r of n){let i=r.borderBoxSize?.length?{width:r.borderBoxSize[0].inlineSize,height:r.borderBoxSize[0].blockSize}:{width:r.contentRect.width,height:r.contentRect.height};i.width!==this._elemSizeCache.get(r.target)?.width&&hE(r.target)&&(t=!0),this._elemSizeCache.set(r.target,i)}t&&this._updatedStickyColumnsParamsToReplay.length&&(this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._stickyColumnsReplayTimeout=setTimeout(()=>{if(!this._destroyed){for(let r of this._updatedStickyColumnsParamsToReplay)this.updateStickyColumns(r.rows,r.stickyStartStates,r.stickyEndStates,!0,!1);this._updatedStickyColumnsParamsToReplay=[],this._stickyColumnsReplayTimeout=null}},0))}};function hE(e){return["cdk-cell","cdk-header-cell","cdk-footer-cell"].some(n=>e.classList.contains(n))}var Li=new g("STICKY_POSITIONING_LISTENER");var nu=(()=>{class e{viewContainer=f(Fe);elementRef=f(U);constructor(){let t=f(dt);t._rowOutlet=this,t._outletAssigned()}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","rowOutlet",""]]})}return e})(),ru=(()=>{class e{viewContainer=f(Fe);elementRef=f(U);constructor(){let t=f(dt);t._headerRowOutlet=this,t._outletAssigned()}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","headerRowOutlet",""]]})}return e})(),iu=(()=>{class e{viewContainer=f(Fe);elementRef=f(U);constructor(){let t=f(dt);t._footerRowOutlet=this,t._outletAssigned()}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","footerRowOutlet",""]]})}return e})(),ou=(()=>{class e{viewContainer=f(Fe);elementRef=f(U);constructor(){let t=f(dt);t._noDataRowOutlet=this,t._outletAssigned()}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","noDataRowOutlet",""]]})}return e})(),su=(()=>{class e{_differs=f($t);_changeDetectorRef=f(pn);_elementRef=f(U);_dir=f(Qd,{optional:!0});_platform=f($e);_viewRepeater;_viewportRuler=f(dg);_injector=f(le);_virtualScrollViewport=f(ug,{optional:!0,host:!0});_positionListener=f(Li,{optional:!0})||f(Li,{optional:!0,skipSelf:!0});_document=f(L);_data;_renderedRange;_onDestroy=new J;_renderRows;_renderChangeSubscription=null;_columnDefsByName=new Map;_rowDefs;_headerRowDefs;_footerRowDefs;_dataDiffer;_defaultRowDef=null;_customColumnDefs=new Set;_customRowDefs=new Set;_customHeaderRowDefs=new Set;_customFooterRowDefs=new Set;_customNoDataRow=null;_headerRowDefChanged=!0;_footerRowDefChanged=!0;_stickyColumnStylesNeedReset=!0;_forceRecalculateCellWidths=!0;_cachedRenderRowsMap=new Map;_isNativeHtmlTable;_stickyStyler;stickyCssClass="cdk-table-sticky";needsPositionStickyOnElement=!0;_isServer;_isShowingNoDataRow=!1;_hasAllOutlets=!1;_hasInitialized=!1;_headerRowStickyUpdates=new J;_footerRowStickyUpdates=new J;_disableVirtualScrolling=!1;_getCellRole(){if(this._cellRoleInternal===void 0){let t=this._elementRef.nativeElement.getAttribute("role");return t==="grid"||t==="treegrid"?"gridcell":"cell"}return this._cellRoleInternal}_cellRoleInternal=void 0;get trackBy(){return this._trackByFn}set trackBy(t){this._trackByFn=t}_trackByFn;get dataSource(){return this._dataSource}set dataSource(t){this._dataSource!==t&&(this._switchDataSource(t),this._changeDetectorRef.markForCheck())}_dataSource;_dataSourceChanges=new J;_dataStream=new J;get multiTemplateDataRows(){return this._multiTemplateDataRows}set multiTemplateDataRows(t){this._multiTemplateDataRows=t,this._rowOutlet&&this._rowOutlet.viewContainer.length&&(this._forceRenderDataRows(),this.updateStickyColumnStyles())}_multiTemplateDataRows=!1;get fixedLayout(){return this._virtualScrollEnabled()?!0:this._fixedLayout}set fixedLayout(t){this._fixedLayout=t,this._forceRecalculateCellWidths=!0,this._stickyColumnStylesNeedReset=!0}_fixedLayout=!1;recycleRows=!1;contentChanged=new ge;viewChange=new At({start:0,end:Number.MAX_VALUE});_rowOutlet;_headerRowOutlet;_footerRowOutlet;_noDataRowOutlet;_contentColumnDefs;_contentRowDefs;_contentHeaderRowDefs;_contentFooterRowDefs;_noDataRow;constructor(){f(new Rs("role"),{optional:!0})||this._elementRef.nativeElement.setAttribute("role","table"),this._isServer=!this._platform.isBrowser,this._isNativeHtmlTable=this._elementRef.nativeElement.nodeName==="TABLE",this._dataDiffer=this._differs.find([]).create((r,i)=>this.trackBy?this.trackBy(i.dataIndex,i.data):i)}ngOnInit(){this._setupStickyStyler(),this._viewportRuler.change().pipe(tt(this._onDestroy)).subscribe(()=>{this._forceRecalculateCellWidths=!0})}ngAfterContentInit(){this._viewRepeater=this.recycleRows||this._virtualScrollEnabled()?new ta:new na,this._virtualScrollEnabled()&&this._setupVirtualScrolling(this._virtualScrollViewport),this._hasInitialized=!0}ngAfterContentChecked(){this._canRender()&&this._render()}ngOnDestroy(){this._stickyStyler?.destroy(),[this._rowOutlet?.viewContainer,this._headerRowOutlet?.viewContainer,this._footerRowOutlet?.viewContainer,this._cachedRenderRowsMap,this._customColumnDefs,this._customRowDefs,this._customHeaderRowDefs,this._customFooterRowDefs,this._columnDefsByName].forEach(t=>{t?.clear()}),this._headerRowDefs=[],this._footerRowDefs=[],this._defaultRowDef=null,this._headerRowStickyUpdates.complete(),this._footerRowStickyUpdates.complete(),this._onDestroy.next(),this._onDestroy.complete(),ea(this.dataSource)&&this.dataSource.disconnect(this)}renderRows(){this._renderRows=this._getAllRenderRows();let t=this._dataDiffer.diff(this._renderRows);if(!t){this._updateNoDataRow(),this.contentChanged.next();return}let r=this._rowOutlet.viewContainer;this._viewRepeater.applyChanges(t,r,(i,o,s)=>this._getEmbeddedViewArgs(i.item,s),i=>i.item.data,i=>{i.operation===lt.INSERTED&&i.context&&this._renderCellTemplateForItem(i.record.item.rowDef,i.context)}),this._updateRowIndexContext(),t.forEachIdentityChange(i=>{let o=r.get(i.currentIndex);o.context.$implicit=i.item.data}),this._updateNoDataRow(),this.contentChanged.next(),this.updateStickyColumnStyles()}addColumnDef(t){this._customColumnDefs.add(t)}removeColumnDef(t){this._customColumnDefs.delete(t)}addRowDef(t){this._customRowDefs.add(t)}removeRowDef(t){this._customRowDefs.delete(t)}addHeaderRowDef(t){this._customHeaderRowDefs.add(t),this._headerRowDefChanged=!0}removeHeaderRowDef(t){this._customHeaderRowDefs.delete(t),this._headerRowDefChanged=!0}addFooterRowDef(t){this._customFooterRowDefs.add(t),this._footerRowDefChanged=!0}removeFooterRowDef(t){this._customFooterRowDefs.delete(t),this._footerRowDefChanged=!0}setNoDataRow(t){this._customNoDataRow=t}updateStickyHeaderRowStyles(){let t=this._getRenderedRows(this._headerRowOutlet);if(this._isNativeHtmlTable){let i=hg(this._headerRowOutlet,"thead");i&&(i.style.display=t.length?"":"none")}let r=this._headerRowDefs.map(i=>i.sticky);this._stickyStyler.clearStickyPositioning(t,["top"]),this._stickyStyler.stickRows(t,r,"top"),this._headerRowDefs.forEach(i=>i.resetStickyChanged())}updateStickyFooterRowStyles(){let t=this._getRenderedRows(this._footerRowOutlet);if(this._isNativeHtmlTable){let i=hg(this._footerRowOutlet,"tfoot");i&&(i.style.display=t.length?"":"none")}let r=this._footerRowDefs.map(i=>i.sticky);this._stickyStyler.clearStickyPositioning(t,["bottom"]),this._stickyStyler.stickRows(t,r,"bottom"),this._stickyStyler.updateStickyFooterContainer(this._elementRef.nativeElement,r),this._footerRowDefs.forEach(i=>i.resetStickyChanged())}updateStickyColumnStyles(){let t=this._getRenderedRows(this._headerRowOutlet),r=this._getRenderedRows(this._rowOutlet),i=this._getRenderedRows(this._footerRowOutlet);(this._isNativeHtmlTable&&!this.fixedLayout||this._stickyColumnStylesNeedReset)&&(this._stickyStyler.clearStickyPositioning([...t,...r,...i],["left","right"]),this._stickyColumnStylesNeedReset=!1),t.forEach((o,s)=>{this._addStickyColumnStyles([o],this._headerRowDefs[s])}),this._rowDefs.forEach(o=>{let s=[];for(let a=0;a<r.length;a++)this._renderRows[a].rowDef===o&&s.push(r[a]);this._addStickyColumnStyles(s,o)}),i.forEach((o,s)=>{this._addStickyColumnStyles([o],this._footerRowDefs[s])}),Array.from(this._columnDefsByName.values()).forEach(o=>o.resetStickyChanged())}stickyColumnsUpdated(t){this._positionListener?.stickyColumnsUpdated(t)}stickyEndColumnsUpdated(t){this._positionListener?.stickyEndColumnsUpdated(t)}stickyHeaderRowsUpdated(t){this._headerRowStickyUpdates.next(t),this._positionListener?.stickyHeaderRowsUpdated(t)}stickyFooterRowsUpdated(t){this._footerRowStickyUpdates.next(t),this._positionListener?.stickyFooterRowsUpdated(t)}_outletAssigned(){!this._hasAllOutlets&&this._rowOutlet&&this._headerRowOutlet&&this._footerRowOutlet&&this._noDataRowOutlet&&(this._hasAllOutlets=!0,this._canRender()&&this._render())}_canRender(){return this._hasAllOutlets&&this._hasInitialized}_render(){this._cacheRowDefs(),this._cacheColumnDefs(),!this._headerRowDefs.length&&!this._footerRowDefs.length&&this._rowDefs.length;let r=this._renderUpdatedColumns()||this._headerRowDefChanged||this._footerRowDefChanged;this._stickyColumnStylesNeedReset=this._stickyColumnStylesNeedReset||r,this._forceRecalculateCellWidths=r,this._headerRowDefChanged&&(this._forceRenderHeaderRows(),this._headerRowDefChanged=!1),this._footerRowDefChanged&&(this._forceRenderFooterRows(),this._footerRowDefChanged=!1),this.dataSource&&this._rowDefs.length>0&&!this._renderChangeSubscription?this._observeRenderChanges():this._stickyColumnStylesNeedReset&&this.updateStickyColumnStyles(),this._checkStickyStates()}_getAllRenderRows(){if(!Array.isArray(this._data)||!this._renderedRange)return[];let t=[],r=Math.min(this._data.length,this._renderedRange.end),i=this._cachedRenderRowsMap;this._cachedRenderRowsMap=new Map;for(let o=this._renderedRange.start;o<r;o++){let s=this._data[o],a=this._getRenderRowsForData(s,o,i.get(s));this._cachedRenderRowsMap.has(s)||this._cachedRenderRowsMap.set(s,new WeakMap);for(let c=0;c<a.length;c++){let l=a[c],d=this._cachedRenderRowsMap.get(l.data);d.has(l.rowDef)?d.get(l.rowDef).push(l):d.set(l.rowDef,[l]),t.push(l)}}return t}_getRenderRowsForData(t,r,i){return this._getRowDefs(t,r).map(s=>{let a=i&&i.has(s)?i.get(s):[];if(a.length){let c=a.shift();return c.dataIndex=r,c}else return{data:t,rowDef:s,dataIndex:r}})}_cacheColumnDefs(){this._columnDefsByName.clear(),ra(this._getOwnDefs(this._contentColumnDefs),this._customColumnDefs).forEach(r=>{this._columnDefsByName.has(r.name),this._columnDefsByName.set(r.name,r)})}_cacheRowDefs(){this._headerRowDefs=ra(this._getOwnDefs(this._contentHeaderRowDefs),this._customHeaderRowDefs),this._footerRowDefs=ra(this._getOwnDefs(this._contentFooterRowDefs),this._customFooterRowDefs),this._rowDefs=ra(this._getOwnDefs(this._contentRowDefs),this._customRowDefs);let t=this._rowDefs.filter(r=>!r.when);this._defaultRowDef=t[0]}_renderUpdatedColumns(){let t=(s,a)=>{let c=!!a.getColumnsDiff();return s||c},r=this._rowDefs.reduce(t,!1);r&&this._forceRenderDataRows();let i=this._headerRowDefs.reduce(t,!1);i&&this._forceRenderHeaderRows();let o=this._footerRowDefs.reduce(t,!1);return o&&this._forceRenderFooterRows(),r||i||o}_switchDataSource(t){this._data=[],ea(this.dataSource)&&this.dataSource.disconnect(this),this._renderChangeSubscription&&(this._renderChangeSubscription.unsubscribe(),this._renderChangeSubscription=null),t||(this._dataDiffer&&this._dataDiffer.diff([]),this._rowOutlet&&this._rowOutlet.viewContainer.clear()),this._dataSource=t}_observeRenderChanges(){if(!this.dataSource)return;let t;ea(this.dataSource)?t=this.dataSource.connect(this):Eo(this.dataSource)?t=this.dataSource:Array.isArray(this.dataSource)&&(t=pt(this.dataSource)),this._renderChangeSubscription=Wr([t,this.viewChange]).pipe(tt(this._onDestroy)).subscribe(([r,i])=>{this._data=r||[],this._renderedRange=i,this._dataStream.next(r),this.renderRows()})}_forceRenderHeaderRows(){this._headerRowOutlet.viewContainer.length>0&&this._headerRowOutlet.viewContainer.clear(),this._headerRowDefs.forEach((t,r)=>this._renderRow(this._headerRowOutlet,t,r)),this.updateStickyHeaderRowStyles()}_forceRenderFooterRows(){this._footerRowOutlet.viewContainer.length>0&&this._footerRowOutlet.viewContainer.clear(),this._footerRowDefs.forEach((t,r)=>this._renderRow(this._footerRowOutlet,t,r)),this.updateStickyFooterRowStyles()}_addStickyColumnStyles(t,r){let i=Array.from(r?.columns||[]).map(a=>{let c=this._columnDefsByName.get(a);return c}),o=i.map(a=>a.sticky),s=i.map(a=>a.stickyEnd);this._stickyStyler.updateStickyColumns(t,o,s,!this.fixedLayout||this._forceRecalculateCellWidths)}_getRenderedRows(t){let r=[];for(let i=0;i<t.viewContainer.length;i++){let o=t.viewContainer.get(i);r.push(o.rootNodes[0])}return r}_getRowDefs(t,r){if(this._rowDefs.length===1)return[this._rowDefs[0]];let i=[];if(this.multiTemplateDataRows)i=this._rowDefs.filter(o=>!o.when||o.when(r,t));else{let o=this._rowDefs.find(s=>s.when&&s.when(r,t))||this._defaultRowDef;o&&i.push(o)}return i.length,i}_getEmbeddedViewArgs(t,r){let i=t.rowDef,o={$implicit:t.data};return{templateRef:i.template,context:o,index:r}}_renderRow(t,r,i,o={}){let s=t.viewContainer.createEmbeddedView(r.template,o,i);return this._renderCellTemplateForItem(r,o),s}_renderCellTemplateForItem(t,r){for(let i of this._getCellTemplates(t))Wn.mostRecentCellOutlet&&Wn.mostRecentCellOutlet._viewContainer.createEmbeddedView(i,r);this._changeDetectorRef.markForCheck()}_updateRowIndexContext(){let t=this._rowOutlet.viewContainer;for(let r=0,i=t.length;r<i;r++){let s=t.get(r).context;s.count=i,s.first=r===0,s.last=r===i-1,s.even=r%2===0,s.odd=!s.even,this.multiTemplateDataRows?(s.dataIndex=this._renderRows[r].dataIndex,s.renderIndex=r):s.index=this._renderRows[r].dataIndex}}_getCellTemplates(t){return!t||!t.columns?[]:Array.from(t.columns,r=>{let i=this._columnDefsByName.get(r);return t.extractCellTemplate(i)})}_forceRenderDataRows(){this._dataDiffer.diff([]),this._rowOutlet.viewContainer.clear(),this.renderRows()}_checkStickyStates(){let t=(r,i)=>r||i.hasStickyChanged();this._headerRowDefs.reduce(t,!1)&&this.updateStickyHeaderRowStyles(),this._footerRowDefs.reduce(t,!1)&&this.updateStickyFooterRowStyles(),Array.from(this._columnDefsByName.values()).reduce(t,!1)&&(this._stickyColumnStylesNeedReset=!0,this.updateStickyColumnStyles())}_setupStickyStyler(){let t=this._dir?this._dir.value:"ltr",r=this._injector;this._stickyStyler=new Kd(this._isNativeHtmlTable,this.stickyCssClass,this._platform.isBrowser,this.needsPositionStickyOnElement,t,this,r),(this._dir?this._dir.change:pt()).pipe(tt(this._onDestroy)).subscribe(i=>{this._stickyStyler.direction=i,this.updateStickyColumnStyles()})}_setupVirtualScrolling(t){let r=typeof requestAnimationFrame<"u"?lo:so;this.viewChange.next({start:0,end:0}),t.renderedRangeStream.pipe(qr(0,r),tt(this._onDestroy)).subscribe(this.viewChange),t.attach({dataStream:this._dataStream,measureRangeSize:(i,o)=>this._measureRangeSize(i,o)}),Wr([t.renderedContentOffset,this._headerRowStickyUpdates]).pipe(tt(this._onDestroy)).subscribe(([i,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let s=0;s<o.elements.length;s++){let a=o.elements[s];if(a){let c=o.offsets[s],l=i!==0?Math.max(i-c,c):-c;for(let d of a)d.style.top=`${-l}px`}}}),Wr([t.renderedContentOffset,this._footerRowStickyUpdates]).pipe(tt(this._onDestroy)).subscribe(([i,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let s=0;s<o.elements.length;s++){let a=o.elements[s];if(a)for(let c of a)c.style.bottom=`${i+o.offsets[s]}px`}})}_getOwnDefs(t){return t.filter(r=>!r._table||r._table===this)}_updateNoDataRow(){let t=this._customNoDataRow||this._noDataRow;if(!t)return;let r=this._rowOutlet.viewContainer.length===0;if(r===this._isShowingNoDataRow)return;let i=this._noDataRowOutlet.viewContainer;if(r){let o=i.createEmbeddedView(t.templateRef),s=o.rootNodes[0];if(o.rootNodes.length===1&&s?.nodeType===this._document.ELEMENT_NODE){s.setAttribute("role","row"),s.classList.add(...t._contentClassNames);let a=s.querySelectorAll(t._cellSelector);for(let c=0;c<a.length;c++)a[c].classList.add(...t._cellClassNames)}}else i.clear();this._isShowingNoDataRow=r,this._changeDetectorRef.markForCheck()}_measureRangeSize(t,r){if(t.start>=t.end||r!=="vertical")return 0;let i=this.viewChange.value,o=this._rowOutlet.viewContainer;t.start<i.start||t.end>i.end;let s=t.start-i.start,a=t.end-t.start,c,l;for(let h=0;h<a;h++){let p=o.get(h+s);if(p&&p.rootNodes.length){c=l=p.rootNodes[0];break}}for(let h=a-1;h>-1;h--){let p=o.get(h+s);if(p&&p.rootNodes.length){l=p.rootNodes[p.rootNodes.length-1];break}}let d=c?.getBoundingClientRect?.(),u=l?.getBoundingClientRect?.();return d&&u?u.bottom-d.top:0}_virtualScrollEnabled(){return!this._disableVirtualScrolling&&this._virtualScrollViewport!=null}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=P({type:e,selectors:[["cdk-table"],["table","cdk-table",""]],contentQueries:function(r,i,o){if(r&1&&$n(o,vg,5)(o,jr,5)(o,aa,5)(o,ji,5)(o,Jd,5),r&2){let s;te(s=ne())&&(i._noDataRow=s.first),te(s=ne())&&(i._contentColumnDefs=s),te(s=ne())&&(i._contentRowDefs=s),te(s=ne())&&(i._contentHeaderRowDefs=s),te(s=ne())&&(i._contentFooterRowDefs=s)}},hostAttrs:[1,"cdk-table"],hostVars:2,hostBindings:function(r,i){r&2&&K("cdk-table-fixed-layout",i.fixedLayout)},inputs:{trackBy:"trackBy",dataSource:"dataSource",multiTemplateDataRows:[2,"multiTemplateDataRows","multiTemplateDataRows",oe],fixedLayout:[2,"fixedLayout","fixedLayout",oe],recycleRows:[2,"recycleRows","recycleRows",oe]},outputs:{contentChanged:"contentChanged"},exportAs:["cdkTable"],features:[Ee([{provide:dt,useExisting:e},{provide:Li,useValue:null}])],ngContentSelectors:dE,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(r,i){r&1&&(je(lE),Y(0),Y(1,1),Ct(2,uE,1,0),Ct(3,fE,7,0)(4,pE,4,0)),r&2&&(O(2),Et(i._isServer?2:-1),O(),Et(i._isNativeHtmlTable?3:4))},dependencies:[ru,nu,ou,iu],styles:[`.cdk-table-fixed-layout {
  table-layout: fixed;
}
`],encapsulation:2})}return e})();function ra(e,n){return e.concat(Array.from(n))}function hg(e,n){let t=n.toUpperCase(),r=e.viewContainer.element.nativeElement;for(;r;){let i=r.nodeType===1?r.nodeName:null;if(i===t)return r;if(i==="TABLE")break;r=r.parentNode}return null}var bg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Q({type:e});static \u0275inj=G({imports:[fg]})}return e})();var mE=[[["caption"]],[["colgroup"],["col"]],"*"],gE=["caption","colgroup, col","*"];function yE(e,n){e&1&&Y(0,2)}function vE(e,n){e&1&&(E(0,"thead",0),de(1,1),T(),E(2,"tbody",2),de(3,3)(4,4),T(),E(5,"tfoot",0),de(6,5),T())}function bE(e,n){e&1&&de(0,1)(1,3)(2,4)(3,5)}var _g=(()=>{class e extends su{stickyCssClass="mat-mdc-table-sticky";needsPositionStickyOnElement=!1;static \u0275fac=(()=>{let t;return function(i){return(t||(t=Oe(e)))(i||e)}})();static \u0275cmp=P({type:e,selectors:[["mat-table"],["table","mat-table",""]],hostAttrs:[1,"mat-mdc-table","mdc-data-table__table"],hostVars:2,hostBindings:function(r,i){r&2&&K("mat-table-fixed-layout",i.fixedLayout)},exportAs:["matTable"],features:[Ee([{provide:su,useExisting:e},{provide:dt,useExisting:e},{provide:Li,useValue:null}]),W],ngContentSelectors:gE,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["role","rowgroup",1,"mdc-data-table__content"],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(r,i){r&1&&(je(mE),Y(0),Y(1,1),Ct(2,yE,1,0),Ct(3,vE,7,0)(4,bE,4,0)),r&2&&(O(2),Et(i._isServer?2:-1),O(),Et(i._isNativeHtmlTable?3:4))},dependencies:[ru,nu,ou,iu],styles:[`.mat-mdc-table-sticky {
  position: sticky !important;
}

mat-table {
  display: block;
}

mat-header-row {
  min-height: var(--mat-table-header-container-height, 56px);
}

mat-row {
  min-height: var(--mat-table-row-item-container-height, 52px);
}

mat-footer-row {
  min-height: var(--mat-table-footer-container-height, 52px);
}

mat-row, mat-header-row, mat-footer-row {
  display: flex;
  border-width: 0;
  border-bottom-width: 1px;
  border-style: solid;
  align-items: center;
  box-sizing: border-box;
}

mat-cell:first-of-type, mat-header-cell:first-of-type, mat-footer-cell:first-of-type {
  padding-left: 24px;
}
[dir=rtl] mat-cell:first-of-type:not(:only-of-type), [dir=rtl] mat-header-cell:first-of-type:not(:only-of-type), [dir=rtl] mat-footer-cell:first-of-type:not(:only-of-type) {
  padding-left: 0;
  padding-right: 24px;
}
mat-cell:last-of-type, mat-header-cell:last-of-type, mat-footer-cell:last-of-type {
  padding-right: 24px;
}
[dir=rtl] mat-cell:last-of-type:not(:only-of-type), [dir=rtl] mat-header-cell:last-of-type:not(:only-of-type), [dir=rtl] mat-footer-cell:last-of-type:not(:only-of-type) {
  padding-right: 0;
  padding-left: 24px;
}

mat-cell, mat-header-cell, mat-footer-cell {
  flex: 1;
  display: flex;
  align-items: center;
  overflow: hidden;
  word-wrap: break-word;
  min-height: inherit;
}

.mat-mdc-table {
  min-width: 100%;
  border: 0;
  border-spacing: 0;
  table-layout: auto;
  white-space: normal;
  background-color: var(--mat-table-background-color, var(--mat-sys-surface));
}

.mat-table-fixed-layout {
  table-layout: fixed;
}

.mdc-data-table__cell {
  box-sizing: border-box;
  overflow: hidden;
  text-align: start;
  text-overflow: ellipsis;
}

.mdc-data-table__cell,
.mdc-data-table__header-cell {
  padding: 0 16px;
}

.mat-mdc-header-row {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  height: var(--mat-table-header-container-height, 56px);
  color: var(--mat-table-header-headline-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-table-header-headline-font, var(--mat-sys-title-small-font, Roboto, sans-serif));
  line-height: var(--mat-table-header-headline-line-height, var(--mat-sys-title-small-line-height));
  font-size: var(--mat-table-header-headline-size, var(--mat-sys-title-small-size, 14px));
  font-weight: var(--mat-table-header-headline-weight, var(--mat-sys-title-small-weight, 500));
}

.mat-mdc-row {
  height: var(--mat-table-row-item-container-height, 52px);
  color: var(--mat-table-row-item-label-text-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
}

.mat-mdc-row,
.mdc-data-table__content {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-table-row-item-label-text-font, var(--mat-sys-body-medium-font, Roboto, sans-serif));
  line-height: var(--mat-table-row-item-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-table-row-item-label-text-size, var(--mat-sys-body-medium-size, 14px));
  font-weight: var(--mat-table-row-item-label-text-weight, var(--mat-sys-body-medium-weight));
}

.mat-mdc-footer-row {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  height: var(--mat-table-footer-container-height, 52px);
  color: var(--mat-table-row-item-label-text-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-table-footer-supporting-text-font, var(--mat-sys-body-medium-font, Roboto, sans-serif));
  line-height: var(--mat-table-footer-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-table-footer-supporting-text-size, var(--mat-sys-body-medium-size, 14px));
  font-weight: var(--mat-table-footer-supporting-text-weight, var(--mat-sys-body-medium-weight));
  letter-spacing: var(--mat-table-footer-supporting-text-tracking, var(--mat-sys-body-medium-tracking));
}

.mat-mdc-header-cell {
  border-bottom-color: var(--mat-table-row-item-outline-color, var(--mat-sys-outline, rgba(0, 0, 0, 0.12)));
  border-bottom-width: var(--mat-table-row-item-outline-width, 1px);
  border-bottom-style: solid;
  letter-spacing: var(--mat-table-header-headline-tracking, var(--mat-sys-title-small-tracking));
  font-weight: inherit;
  line-height: inherit;
  box-sizing: border-box;
  text-overflow: ellipsis;
  overflow: hidden;
  outline: none;
  text-align: start;
}
.mdc-data-table__row:last-child > .mat-mdc-header-cell {
  border-bottom: none;
}

.mat-mdc-cell {
  border-bottom-color: var(--mat-table-row-item-outline-color, var(--mat-sys-outline, rgba(0, 0, 0, 0.12)));
  border-bottom-width: var(--mat-table-row-item-outline-width, 1px);
  border-bottom-style: solid;
  letter-spacing: var(--mat-table-row-item-label-text-tracking, var(--mat-sys-body-medium-tracking));
  line-height: inherit;
}
.mdc-data-table__row:last-child > .mat-mdc-cell {
  border-bottom: none;
}

.mat-mdc-footer-cell {
  letter-spacing: var(--mat-table-row-item-label-text-tracking, var(--mat-sys-body-medium-tracking));
}

mat-row.mat-mdc-row,
mat-header-row.mat-mdc-header-row,
mat-footer-row.mat-mdc-footer-row {
  border-bottom: none;
}

.mat-mdc-table tbody,
.mat-mdc-table tfoot,
.mat-mdc-table thead,
.mat-mdc-cell,
.mat-mdc-footer-cell,
.mat-mdc-header-row,
.mat-mdc-row,
.mat-mdc-footer-row,
.mat-mdc-table .mat-mdc-header-cell {
  background: inherit;
}

.mat-mdc-table mat-header-row.mat-mdc-header-row,
.mat-mdc-table mat-row.mat-mdc-row,
.mat-mdc-table mat-footer-row.mat-mdc-footer-cell {
  height: unset;
}

mat-header-cell.mat-mdc-header-cell,
mat-cell.mat-mdc-cell,
mat-footer-cell.mat-mdc-footer-cell {
  align-self: stretch;
}
`],encapsulation:2})}return e})(),Dg=(()=>{class e extends oa{static \u0275fac=(()=>{let t;return function(i){return(t||(t=Oe(e)))(i||e)}})();static \u0275dir=I({type:e,selectors:[["","matCellDef",""]],features:[Ee([{provide:oa,useExisting:e}]),W]})}return e})(),wg=(()=>{class e extends sa{static \u0275fac=(()=>{let t;return function(i){return(t||(t=Oe(e)))(i||e)}})();static \u0275dir=I({type:e,selectors:[["","matHeaderCellDef",""]],features:[Ee([{provide:sa,useExisting:e}]),W]})}return e})();var Cg=(()=>{class e extends jr{get name(){return this._name}set name(t){this._setNameInput(t)}_updateColumnCssClassName(){super._updateColumnCssClassName(),this._columnCssClassName.push(`mat-column-${this.cssClassFriendlyName}`)}static \u0275fac=(()=>{let t;return function(i){return(t||(t=Oe(e)))(i||e)}})();static \u0275dir=I({type:e,selectors:[["","matColumnDef",""]],inputs:{name:[0,"matColumnDef","name"]},features:[Ee([{provide:jr,useExisting:e}]),W]})}return e})(),Eg=(()=>{class e extends gg{static \u0275fac=(()=>{let t;return function(i){return(t||(t=Oe(e)))(i||e)}})();static \u0275dir=I({type:e,selectors:[["mat-header-cell"],["th","mat-header-cell",""]],hostAttrs:["role","columnheader",1,"mat-mdc-header-cell","mdc-data-table__header-cell"],features:[W]})}return e})();var Ig=(()=>{class e extends yg{static \u0275fac=(()=>{let t;return function(i){return(t||(t=Oe(e)))(i||e)}})();static \u0275dir=I({type:e,selectors:[["mat-cell"],["td","mat-cell",""]],hostAttrs:[1,"mat-mdc-cell","mdc-data-table__cell"],features:[W]})}return e})();var Tg=(()=>{class e extends ji{static \u0275fac=(()=>{let t;return function(i){return(t||(t=Oe(e)))(i||e)}})();static \u0275dir=I({type:e,selectors:[["","matHeaderRowDef",""]],inputs:{columns:[0,"matHeaderRowDef","columns"],sticky:[2,"matHeaderRowDefSticky","sticky",oe]},features:[Ee([{provide:ji,useExisting:e}]),W]})}return e})();var Mg=(()=>{class e extends aa{static \u0275fac=(()=>{let t;return function(i){return(t||(t=Oe(e)))(i||e)}})();static \u0275dir=I({type:e,selectors:[["","matRowDef",""]],inputs:{columns:[0,"matRowDefColumns","columns"],when:[0,"matRowDefWhen","when"]},features:[Ee([{provide:aa,useExisting:e}]),W]})}return e})(),Sg=(()=>{class e extends eu{static \u0275fac=(()=>{let t;return function(i){return(t||(t=Oe(e)))(i||e)}})();static \u0275cmp=P({type:e,selectors:[["mat-header-row"],["tr","mat-header-row",""]],hostAttrs:["role","row",1,"mat-mdc-header-row","mdc-data-table__header-row"],exportAs:["matHeaderRow"],features:[Ee([{provide:eu,useExisting:e}]),W],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(r,i){r&1&&de(0,0)},dependencies:[Wn],encapsulation:2})}return e})();var xg=(()=>{class e extends tu{static \u0275fac=(()=>{let t;return function(i){return(t||(t=Oe(e)))(i||e)}})();static \u0275cmp=P({type:e,selectors:[["mat-row"],["tr","mat-row",""]],hostAttrs:["role","row",1,"mat-mdc-row","mdc-data-table__row"],exportAs:["matRow"],features:[Ee([{provide:tu,useExisting:e}]),W],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(r,i){r&1&&de(0,0)},dependencies:[Wn],encapsulation:2})}return e})();var Rg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Q({type:e});static \u0275inj=G({imports:[bg,Ae]})}return e})();var DE=["*"];var wE=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],CE=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],EE=new g("MAT_CARD_CONFIG"),Ag=(()=>{class e{appearance;constructor(){let t=f(EE,{optional:!0});this.appearance=t?.appearance||"raised"}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=P({type:e,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(r,i){r&2&&K("mat-mdc-card-outlined",i.appearance==="outlined")("mdc-card--outlined",i.appearance==="outlined")("mat-mdc-card-filled",i.appearance==="filled")("mdc-card--filled",i.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:DE,decls:1,vars:0,template:function(r,i){r&1&&(je(),Y(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return e})(),Ng=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return e})();var kg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return e})(),Og=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["mat-card-subtitle"],["","mat-card-subtitle",""],["","matCardSubtitle",""]],hostAttrs:[1,"mat-mdc-card-subtitle"]})}return e})(),Fg=(()=>{class e{align="start";static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(r,i){r&2&&K("mat-mdc-card-actions-align-end",i.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return e})(),Pg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=P({type:e,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:CE,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(r,i){r&1&&(je(wE),Y(0),Bt(1,"div",0),Y(2,1),Vt(),Y(3,2))},encapsulation:2,changeDetection:0})}return e})();var Lg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Q({type:e});static \u0275inj=G({imports:[Ae]})}return e})();function Bi(e){return e.buttons===0||e.detail===0}function Vi(e){let n=e.touches&&e.touches[0]||e.changedTouches&&e.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var au;function jg(){if(au==null){let e=typeof document<"u"?document.head:null;au=!!(e&&(e.createShadowRoot||e.attachShadow))}return au}function cu(e){if(jg()){let n=e.getRootNode?e.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function ut(e){return e.composedPath?e.composedPath()[0]:e.target}var Hi;function Bg(){if(Hi==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Hi=!0}))}finally{Hi=Hi||!1}return Hi}function Br(e){return Bg()?e:!!e.capture}var Vg=new g("cdk-input-modality-detector-options"),Hg={ignoreKeys:[18,17,224,91,16]},Ug=650,lu={passive:!0,capture:!0},$g=(()=>{class e{_platform=f($e);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new At(null);_options;_lastTouchMs=0;_onKeydown=t=>{this._options?.ignoreKeys?.some(r=>r===t.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=ut(t))};_onMousedown=t=>{Date.now()-this._lastTouchMs<Ug||(this._modality.next(Bi(t)?"keyboard":"mouse"),this._mostRecentTarget=ut(t))};_onTouchstart=t=>{if(Vi(t)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=ut(t)};constructor(){let t=f(V),r=f(L),i=f(Vg,{optional:!0});if(this._options=q(q({},Hg),i),this.modalityDetected=this._modality.pipe(za(1)),this.modalityChanged=this.modalityDetected.pipe(Io()),this._platform.isBrowser){let o=f(He).createRenderer(null,null);this._listenerCleanups=t.runOutsideAngular(()=>[o.listen(r,"keydown",this._onKeydown,lu),o.listen(r,"mousedown",this._onMousedown,lu),o.listen(r,"touchstart",this._onTouchstart,lu)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(t=>t())}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Ui=(function(e){return e[e.IMMEDIATE=0]="IMMEDIATE",e[e.EVENTUAL=1]="EVENTUAL",e})(Ui||{}),zg=new g("cdk-focus-monitor-default-options"),ca=Br({passive:!0,capture:!0}),$i=(()=>{class e{_ngZone=f(V);_platform=f($e);_inputModalityDetector=f($g);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=f(L);_stopInputModalityDetector=new J;constructor(){let t=f(zg,{optional:!0});this._detectionMode=t?.detectionMode||Ui.IMMEDIATE}_rootNodeFocusAndBlurListener=t=>{let r=ut(t);for(let i=r;i;i=i.parentElement)t.type==="focus"?this._onFocus(t,i):this._onBlur(t,i)};monitor(t,r=!1){let i=gn(t);if(!this._platform.isBrowser||i.nodeType!==1)return pt();let o=cu(i)||this._document,s=this._elementInfo.get(i);if(s)return r&&(s.checkChildren=!0),s.subject;let a={checkChildren:r,subject:new J,rootNode:o};return this._elementInfo.set(i,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(t){let r=gn(t),i=this._elementInfo.get(r);i&&(i.subject.complete(),this._setClasses(r),this._elementInfo.delete(r),this._removeGlobalListeners(i))}focusVia(t,r,i){let o=gn(t),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,r,c)):(this._setOrigin(r),typeof o.focus=="function"&&o.focus(i))}ngOnDestroy(){this._elementInfo.forEach((t,r)=>this.stopMonitoring(r))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(t){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(t)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:t&&this._isLastInteractionFromInputLabel(t)?"mouse":"program"}_shouldBeAttributedToTouch(t){return this._detectionMode===Ui.EVENTUAL||!!t?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(t,r){t.classList.toggle("cdk-focused",!!r),t.classList.toggle("cdk-touch-focused",r==="touch"),t.classList.toggle("cdk-keyboard-focused",r==="keyboard"),t.classList.toggle("cdk-mouse-focused",r==="mouse"),t.classList.toggle("cdk-program-focused",r==="program")}_setOrigin(t,r=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=t,this._originFromTouchInteraction=t==="touch"&&r,this._detectionMode===Ui.IMMEDIATE){clearTimeout(this._originTimeoutId);let i=this._originFromTouchInteraction?Ug:1;this._originTimeoutId=setTimeout(()=>this._origin=null,i)}})}_onFocus(t,r){let i=this._elementInfo.get(r),o=ut(t);!i||!i.checkChildren&&r!==o||this._originChanged(r,this._getFocusOrigin(o),i)}_onBlur(t,r){let i=this._elementInfo.get(r);!i||i.checkChildren&&t.relatedTarget instanceof Node&&r.contains(t.relatedTarget)||(this._setClasses(r),this._emitOrigin(i,null))}_emitOrigin(t,r){t.subject.observers.length&&this._ngZone.run(()=>t.subject.next(r))}_registerGlobalListeners(t){if(!this._platform.isBrowser)return;let r=t.rootNode,i=this._rootNodeFocusListenerCount.get(r)||0;i||this._ngZone.runOutsideAngular(()=>{r.addEventListener("focus",this._rootNodeFocusAndBlurListener,ca),r.addEventListener("blur",this._rootNodeFocusAndBlurListener,ca)}),this._rootNodeFocusListenerCount.set(r,i+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(tt(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(t){let r=t.rootNode;if(this._rootNodeFocusListenerCount.has(r)){let i=this._rootNodeFocusListenerCount.get(r);i>1?this._rootNodeFocusListenerCount.set(r,i-1):(r.removeEventListener("focus",this._rootNodeFocusAndBlurListener,ca),r.removeEventListener("blur",this._rootNodeFocusAndBlurListener,ca),this._rootNodeFocusListenerCount.delete(r))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(t,r,i){this._setClasses(t,r),this._emitOrigin(i,r),this._lastFocusOrigin=r}_getClosestElementsInfo(t){let r=[];return this._elementInfo.forEach((i,o)=>{(o===t||i.checkChildren&&o.contains(t))&&r.push([o,i])}),r}_isLastInteractionFromInputLabel(t){let{_mostRecentTarget:r,mostRecentModality:i}=this._inputModalityDetector;if(i!=="mouse"||!r||r===t||t.nodeName!=="INPUT"&&t.nodeName!=="TEXTAREA"||t.disabled)return!1;let o=t.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(r))return!0}return!1}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var la=new WeakMap,yn=(()=>{class e{_appRef;_injector=f(le);_environmentInjector=f(ye);load(t){let r=this._appRef=this._appRef||this._injector.get(Vn),i=la.get(r);i||(i={loaders:new Set,refs:[]},la.set(r,i),r.onDestroy(()=>{la.get(r)?.refs.forEach(o=>o.destroy()),la.delete(r)})),i.loaders.has(t)||(i.loaders.add(t),i.refs.push(Cm(t,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Gg=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=P({type:e,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(r,i){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return e})();var Wg=new Set,qn,du=(()=>{class e{_platform=f($e);_nonce=f(Bn,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):ME}matchMedia(t){return(this._platform.WEBKIT||this._platform.BLINK)&&TE(t,this._nonce),this._matchMedia(t)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function TE(e,n){if(!Wg.has(e))try{qn||(qn=document.createElement("style"),n&&qn.setAttribute("nonce",n),qn.setAttribute("type","text/css"),document.head.appendChild(qn)),qn.sheet&&(qn.sheet.insertRule(`@media ${e} {body{ }}`,0),Wg.add(e))}catch(t){console.error(t)}}function ME(e){return{matches:e==="all"||e==="",media:e,addListener:()=>{},removeListener:()=>{}}}var uu={},zi=class e{_appId=f(Rr);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,t=!1){return this._appId!=="ng"&&(n+=this._appId),uu.hasOwnProperty(n)||(uu[n]=0),`${n}${t?e._infix+"-":""}${uu[n]++}`}static \u0275fac=function(t){return new(t||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})};var SE=new g("MATERIAL_ANIMATIONS"),qg=null;function fu(){return f(SE,{optional:!0})?.animationsDisabled||f(Bl,{optional:!0})==="NoopAnimations"?"di-disabled":(qg??=f(du).matchMedia("(prefers-reduced-motion)").matches,qg?"reduced-motion":"enabled")}function Vr(){return fu()!=="enabled"}var Ke=(function(e){return e[e.FADING_IN=0]="FADING_IN",e[e.VISIBLE=1]="VISIBLE",e[e.FADING_OUT=2]="FADING_OUT",e[e.HIDDEN=3]="HIDDEN",e})(Ke||{}),pu=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Ke.HIDDEN;constructor(n,t,r,i=!1){this._renderer=n,this.element=t,this.config=r,this._animationForciblyDisabledThroughCss=i}fadeOut(){this._renderer.fadeOutRipple(this)}},Zg=Br({passive:!0,capture:!0}),hu=class{_events=new Map;addHandler(n,t,r,i){let o=this._events.get(t);if(o){let s=o.get(r);s?s.add(i):o.set(r,new Set([i]))}else this._events.set(t,new Map([[r,new Set([i])]])),n.runOutsideAngular(()=>{document.addEventListener(t,this._delegateEventHandler,Zg)})}removeHandler(n,t,r){let i=this._events.get(n);if(!i)return;let o=i.get(t);o&&(o.delete(r),o.size===0&&i.delete(t),i.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,Zg)))}_delegateEventHandler=n=>{let t=ut(n);t&&this._events.get(n.type)?.forEach((r,i)=>{(i===t||i.contains(t))&&r.forEach(o=>o.handleEvent(n))})}},Gi={enterDuration:225,exitDuration:150},xE=800,Qg=Br({passive:!0,capture:!0}),Yg=["mousedown","touchstart"],Kg=["mouseup","mouseleave","touchend","touchcancel"],RE=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=P({type:e,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(r,i){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return e})(),da=class e{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new hu;constructor(n,t,r,i,o){this._target=n,this._ngZone=t,this._platform=i,i.isBrowser&&(this._containerElement=gn(r)),o&&o.get(yn).load(RE)}fadeInRipple(n,t,r={}){let i=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=q(q({},Gi),r.animation);r.centered&&(n=i.left+i.width/2,t=i.top+i.height/2);let s=r.radius||AE(n,t,i),a=n-i.left,c=t-i.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,r.color!=null&&(d.style.backgroundColor=r.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),h=u.transitionProperty,p=u.transitionDuration,m=h==="none"||p==="0s"||p==="0s, 0s"||i.width===0&&i.height===0,D=new pu(this,d,r,m);d.style.transform="scale3d(1, 1, 1)",D.state=Ke.FADING_IN,r.persistent||(this._mostRecentTransientRipple=D);let y=null;return!m&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let v=()=>{y&&(y.fallbackTimer=null),clearTimeout(Xe),this._finishRippleTransition(D)},se=()=>this._destroyRipple(D),Xe=setTimeout(se,l+100);d.addEventListener("transitionend",v),d.addEventListener("transitioncancel",se),y={onTransitionEnd:v,onTransitionCancel:se,fallbackTimer:Xe}}),this._activeRipples.set(D,y),(m||!l)&&this._finishRippleTransition(D),D}fadeOutRipple(n){if(n.state===Ke.FADING_OUT||n.state===Ke.HIDDEN)return;let t=n.element,r=q(q({},Gi),n.config.animation);t.style.transitionDuration=`${r.exitDuration}ms`,t.style.opacity="0",n.state=Ke.FADING_OUT,(n._animationForciblyDisabledThroughCss||!r.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let t=gn(n);!this._platform.isBrowser||!t||t===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=t,Yg.forEach(r=>{e._eventManager.addHandler(this._ngZone,r,t,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{Kg.forEach(t=>{this._triggerElement.addEventListener(t,this,Qg)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===Ke.FADING_IN?this._startFadeOutTransition(n):n.state===Ke.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let t=n===this._mostRecentTransientRipple,{persistent:r}=n.config;n.state=Ke.VISIBLE,!r&&(!t||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let t=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=Ke.HIDDEN,t!==null&&(n.element.removeEventListener("transitionend",t.onTransitionEnd),n.element.removeEventListener("transitioncancel",t.onTransitionCancel),t.fallbackTimer!==null&&clearTimeout(t.fallbackTimer)),n.element.remove()}_onMousedown(n){let t=Bi(n),r=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+xE;!this._target.rippleDisabled&&!t&&!r&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Vi(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let t=n.changedTouches;if(t)for(let r=0;r<t.length;r++)this.fadeInRipple(t[r].clientX,t[r].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let t=n.state===Ke.VISIBLE||n.config.terminateOnPointerUp&&n.state===Ke.FADING_IN;!n.config.persistent&&t&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(Yg.forEach(t=>e._eventManager.removeHandler(t,n,this)),this._pointerUpEventsRegistered&&(Kg.forEach(t=>n.removeEventListener(t,this,Qg)),this._pointerUpEventsRegistered=!1))}};function AE(e,n,t){let r=Math.max(Math.abs(e-t.left),Math.abs(e-t.right)),i=Math.max(Math.abs(n-t.top),Math.abs(n-t.bottom));return Math.sqrt(r*r+i*i)}var ua=new g("mat-ripple-global-options");var NE={capture:!0},kE=["focus","mousedown","mouseenter","touchstart"],mu="mat-ripple-loader-uninitialized",gu="mat-ripple-loader-class-name",Xg="mat-ripple-loader-centered",fa="mat-ripple-loader-disabled",pa=(()=>{class e{_document=f(L);_animationsDisabled=Vr();_globalRippleOptions=f(ua,{optional:!0});_platform=f($e);_ngZone=f(V);_injector=f(le);_eventCleanups;_hosts=new Map;constructor(){let t=f(He).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>kE.map(r=>t.listen(this._document,r,this._onInteraction,NE)))}ngOnDestroy(){let t=this._hosts.keys();for(let r of t)this.destroyRipple(r);this._eventCleanups.forEach(r=>r())}configureRipple(t,r){t.setAttribute(mu,this._globalRippleOptions?.namespace??""),(r.className||!t.hasAttribute(gu))&&t.setAttribute(gu,r.className||""),r.centered&&t.setAttribute(Xg,""),r.disabled&&t.setAttribute(fa,"")}setDisabled(t,r){let i=this._hosts.get(t);i?(i.target.rippleDisabled=r,!r&&!i.hasSetUpEvents&&(i.hasSetUpEvents=!0,i.renderer.setupTriggerEvents(t))):r?t.setAttribute(fa,""):t.removeAttribute(fa)}_onInteraction=t=>{let r=ut(t);if(r instanceof HTMLElement){let i=r.closest(`[${mu}="${this._globalRippleOptions?.namespace??""}"]`);i&&this._createRipple(i)}};_createRipple(t){if(!this._document||this._hosts.has(t))return;t.querySelector(".mat-ripple")?.remove();let r=this._document.createElement("span");r.classList.add("mat-ripple",t.getAttribute(gu)),t.append(r);let i=this._globalRippleOptions,o=this._animationsDisabled?0:i?.animation?.enterDuration??Gi.enterDuration,s=this._animationsDisabled?0:i?.animation?.exitDuration??Gi.exitDuration,a={rippleDisabled:this._animationsDisabled||i?.disabled||t.hasAttribute(fa),rippleConfig:{centered:t.hasAttribute(Xg),terminateOnPointerUp:i?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new da(a,this._ngZone,r,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(t),this._hosts.set(t,{target:a,renderer:c,hasSetUpEvents:l}),t.removeAttribute(mu)}destroyRipple(t){let r=this._hosts.get(t);r&&(r.renderer._removeTriggerEvents(),this._hosts.delete(t))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Wi=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=P({type:e,selectors:[["structural-styles"]],decls:0,vars:0,template:function(r,i){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return e})();var OE=new g("MAT_BUTTON_CONFIG");function Jg(e){return e==null?void 0:hn(e)}var ey=(()=>{class e{_elementRef=f(U);_ngZone=f(V);_animationsDisabled=Vr();_config=f(OE,{optional:!0});_focusMonitor=f($i);_cleanupClick;_renderer=f(kr);_rippleLoader=f(pa);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(t){this._disableRipple=t,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(t){this.tabIndex=t}constructor(){f(yn).load(Wi);let t=this._elementRef.nativeElement;this._isAnchor=t.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(t,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(t="program",r){t?this._focusMonitor.focusVia(this._elementRef.nativeElement,t,r):this._elementRef.nativeElement.focus(r)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",t=>{this.disabled&&(t.preventDefault(),t.stopImmediatePropagation())}))}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(r,i){r&2&&(Re("disabled",i._getDisabledAttribute())("aria-disabled",i._getAriaDisabled())("tabindex",i._getTabIndex()),zn(i.color?"mat-"+i.color:""),K("mat-mdc-button-disabled",i.disabled)("mat-mdc-button-disabled-interactive",i.disabledInteractive)("mat-unthemed",!i.color)("_mat-animation-noopable",i._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",oe],disabled:[2,"disabled","disabled",oe],ariaDisabled:[2,"aria-disabled","ariaDisabled",oe],disabledInteractive:[2,"disabledInteractive","disabledInteractive",oe],tabIndex:[2,"tabIndex","tabIndex",Jg],_tabindex:[2,"tabindex","_tabindex",Jg]}})}return e})();var ha=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Q({type:e});static \u0275inj=G({imports:[Ae]})}return e})();var FE=["matButton",""],PE=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],LE=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var ty=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),ny=(()=>{class e extends ey{get appearance(){return this._appearance}set appearance(t){this.setAppearance(t||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let t=jE(this._elementRef.nativeElement);t&&this.setAppearance(t)}setAppearance(t){if(t===this._appearance)return;let r=this._elementRef.nativeElement.classList,i=this._appearance?ty.get(this._appearance):null,o=ty.get(t);i&&r.remove(...i),r.add(...o),this._appearance=t}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=P({type:e,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[W],attrs:FE,ngContentSelectors:LE,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(r,i){r&1&&(je(PE),Hn(0,"span",0),Y(1),Bt(2,"span",1),Y(3,1),Vt(),Y(4,2),Hn(5,"span",2)(6,"span",3)),r&2&&K("mdc-button__ripple",!i._isFab)("mdc-fab__ripple",i._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return e})();function jE(e){return e.hasAttribute("mat-raised-button")?"elevated":e.hasAttribute("mat-stroked-button")?"outlined":e.hasAttribute("mat-flat-button")?"filled":e.hasAttribute("mat-button")?"text":null}var ry=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Q({type:e});static \u0275inj=G({imports:[ha,Ae]})}return e})();var VE=["determinateSpinner"];function HE(e,n){if(e&1&&(Dr(),E(0,"svg",11),Le(1,"circle",12),T()),e&2){let t=Qe();Re("viewBox",t._viewBox()),O(),Ht("stroke-dasharray",t._strokeCircumference(),"px")("stroke-dashoffset",t._strokeCircumference()/2,"px")("stroke-width",t._circleStrokeWidth(),"%"),Re("r",t._circleRadius())}}var UE=new g("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:iy})}),iy=100,$E=10,oy=(()=>{class e{_elementRef=f(U);_noopAnimations;get color(){return this._color||this._defaultColor}set color(t){this._color=t}_color;_defaultColor="primary";_determinateCircle;constructor(){let t=f(UE),r=fu(),i=this._elementRef.nativeElement;this._noopAnimations=r==="di-disabled"&&!!t&&!t._forceAnimations,this.mode=i.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&r==="reduced-motion"&&i.classList.add("mat-progress-spinner-reduced-motion"),t&&(t.color&&(this.color=this._defaultColor=t.color),t.diameter&&(this.diameter=t.diameter),t.strokeWidth&&(this.strokeWidth=t.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(t){this._value=Math.max(0,Math.min(100,t||0))}_value=0;get diameter(){return this._diameter}set diameter(t){this._diameter=t||0}_diameter=iy;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(t){this._strokeWidth=t||0}_strokeWidth;_circleRadius(){return(this.diameter-$E)/2}_viewBox(){let t=this._circleRadius()*2+this.strokeWidth;return`0 0 ${t} ${t}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=P({type:e,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(r,i){if(r&1&&fn(VE,5),r&2){let o;te(o=ne())&&(i._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(r,i){r&2&&(Re("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",i.mode==="determinate"?i.value:null)("mode",i.mode),zn("mat-"+i.color),Ht("width",i.diameter,"px")("height",i.diameter,"px")("--mat-progress-spinner-size",i.diameter+"px")("--mat-progress-spinner-active-indicator-width",i.diameter+"px"),K("_mat-animation-noopable",i._noopAnimations)("mdc-circular-progress--indeterminate",i.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",hn],diameter:[2,"diameter","diameter",hn],strokeWidth:[2,"strokeWidth","strokeWidth",hn]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(r,i){if(r&1&&(Pe(0,HE,2,8,"ng-template",null,0,bd),E(2,"div",2,1),Dr(),E(4,"svg",3),Le(5,"circle",4),T()(),Yo(),E(6,"div",5)(7,"div",6)(8,"div",7),de(9,8),T(),E(10,"div",9),de(11,8),T(),E(12,"div",10),de(13,8),T()()()),r&2){let o=gd(1);O(4),Re("viewBox",i._viewBox()),O(),Ht("stroke-dasharray",i._strokeCircumference(),"px")("stroke-dashoffset",i._strokeDashOffset(),"px")("stroke-width",i._circleStrokeWidth(),"%"),Re("r",i._circleRadius()),O(4),Ce("ngTemplateOutlet",o),O(2),Ce("ngTemplateOutlet",o),O(2),Ce("ngTemplateOutlet",o)}},dependencies:[Rd],styles:[`.mat-mdc-progress-spinner {
  --mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--mat-progress-spinner-active-indicator-color, var(--mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2,changeDetection:0})}return e})();var sy=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Q({type:e});static \u0275inj=G({imports:[Ae]})}return e})();var ay=(()=>{class e{isErrorState(t,r){return!!(t&&t.invalid&&(t.touched||r&&r.submitted))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var GE=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],WE=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function qE(e,n){e&1&&(E(0,"span",3),Y(1,1),T())}function ZE(e,n){e&1&&(E(0,"span",6),Y(1,2),T())}var QE=new g("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),cy=new g("MatChipAvatar"),ly=new g("MatChipTrailingIcon"),dy=new g("MatChipEdit"),uy=new g("MatChipRemove"),py=new g("MatChip"),hy=(()=>{class e{_elementRef=f(U);_parentChip=f(py);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(t){this._disabled=t}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){f(yn).load(Wi),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(r){return new(r||e)};static \u0275dir=I({type:e,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(r,i){r&2&&(Re("disabled",i._getDisabledAttribute())("aria-disabled",i.disabled),K("mdc-evolution-chip__action--primary",i._isPrimary)("mdc-evolution-chip__action--secondary",!i._isPrimary)("mdc-evolution-chip__action--trailing",!i._isPrimary&&!i._isLeading))},inputs:{disabled:[2,"disabled","disabled",oe],tabIndex:[2,"tabIndex","tabIndex",t=>t==null?-1:hn(t)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return e})(),YE=(()=>{class e extends hy{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(t){!this.disabled&&this._isPrimary&&(t.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(t){(t.keyCode===13||t.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(t.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let t;return function(i){return(t||(t=Oe(e)))(i||e)}})();static \u0275dir=I({type:e,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(r,i){r&1&&Un("click",function(s){return i._handleClick(s)})("keydown",function(s){return i._handleKeydown(s)}),r&2&&(Re("tabindex",i._getTabindex()),K("mdc-evolution-chip__action--presentational",!1))},features:[W]})}return e})();var my=(()=>{class e{_changeDetectorRef=f(pn);_elementRef=f(U);_tagName=f(Dm);_ngZone=f(V);_focusMonitor=f($i);_globalRippleOptions=f(ua,{optional:!0});_document=f(L);_onFocus=new J;_onBlur=new J;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=Vr();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=f(zi).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(t){this._value=t}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(t){this._disabled=t}_disabled=!1;removed=new ge;destroyed=new ge;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=f(pa);_injector=f(le);constructor(){let t=f(yn);t.load(Wi),t.load(Gg),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=Ha(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe(),this.destroyed.emit({chip:this}),this.destroyed.complete()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(t){(t.keyCode===8&&!t.repeat||t.keyCode===46)&&(t.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(t){return this._getActions().find(r=>{let i=r._elementRef.nativeElement;return i===t||i.contains(t)})}_getActions(){let t=[];return this.editIcon&&t.push(this.editIcon),this.primaryAction&&t.push(this.primaryAction),this.removeIcon&&t.push(this.removeIcon),t}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(t){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(t=>{let r=t!==null;r!==this._hasFocusInternal&&(this._hasFocusInternal=r,r?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=P({type:e,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(r,i,o){if(r&1&&$n(o,cy,5)(o,dy,5)(o,ly,5)(o,uy,5)(o,cy,5)(o,ly,5)(o,dy,5)(o,uy,5),r&2){let s;te(s=ne())&&(i.leadingIcon=s.first),te(s=ne())&&(i.editIcon=s.first),te(s=ne())&&(i.trailingIcon=s.first),te(s=ne())&&(i.removeIcon=s.first),te(s=ne())&&(i._allLeadingIcons=s),te(s=ne())&&(i._allTrailingIcons=s),te(s=ne())&&(i._allEditIcons=s),te(s=ne())&&(i._allRemoveIcons=s)}},viewQuery:function(r,i){if(r&1&&fn(YE,5),r&2){let o;te(o=ne())&&(i.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(r,i){r&1&&Un("keydown",function(s){return i._handleKeydown(s)}),r&2&&(Ss("id",i.id),Re("role",i.role)("aria-label",i.ariaLabel),zn("mat-"+(i.color||"primary")),K("mdc-evolution-chip",!i._isBasicChip)("mdc-evolution-chip--disabled",i.disabled)("mdc-evolution-chip--with-trailing-action",i._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",i.leadingIcon)("mdc-evolution-chip--with-primary-icon",i.leadingIcon)("mdc-evolution-chip--with-avatar",i.leadingIcon)("mat-mdc-chip-with-avatar",i.leadingIcon)("mat-mdc-chip-highlighted",i.highlighted)("mat-mdc-chip-disabled",i.disabled)("mat-mdc-basic-chip",i._isBasicChip)("mat-mdc-standard-chip",!i._isBasicChip)("mat-mdc-chip-with-trailing-icon",i._hasTrailingIcon())("_mat-animation-noopable",i._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",oe],highlighted:[2,"highlighted","highlighted",oe],disableRipple:[2,"disableRipple","disableRipple",oe],disabled:[2,"disabled","disabled",oe]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[Ee([{provide:py,useExisting:e}])],ngContentSelectors:WE,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(r,i){r&1&&(je(GE),Le(0,"span",0),E(1,"span",1)(2,"span",2),Ct(3,qE,2,0,"span",3),E(4,"span",4),Y(5),Le(6,"span",5),T()()(),Ct(7,ZE,2,0,"span",6)),r&2&&(O(3),Et(i.leadingIcon?3:-1),O(4),Et(i._hasTrailingIcon()?7:-1))},dependencies:[hy],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-outline-width, 1px);
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));
}
.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));
  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--mat-chip-with-avatar-avatar-size, 24px);
  height: var(--mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  height: var(--mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--mat-chip-with-icon-icon-size, 18px);
  height: var(--mat-chip-with-icon-icon-size, 18px);
  font-size: var(--mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2,changeDetection:0})}return e})();var gy=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Q({type:e});static \u0275inj=G({providers:[ay,{provide:QE,useValue:{separatorKeyCodes:[13]}}],imports:[ha,Ae]})}return e})();var ma=class e{constructor(n){this.http=n}http;apiUrl="https://sampleapi20260706g3-bvdacte9b0dvhudv.canadacentral-01.azurewebsites.net/Weatherforecast";getForecasts(){return this.http.get(this.apiUrl)}static \u0275fac=function(t){return new(t||e)(x(Js))};static \u0275prov=_({token:e,factory:e.\u0275fac,providedIn:"root"})};function JE(e,n){if(e&1&&(E(0,"mat-card-subtitle"),be(1),T()),e&2){let t=Qe();O(),Ut(" Total Forecast Records: ",t.forecasts.length," ")}}function e0(e,n){e&1&&(E(0,"div",7),Le(1,"mat-spinner",8),E(2,"p"),be(3,"Loading..."),T()())}function t0(e,n){if(e&1&&(E(0,"div",9)(1,"mat-chip",10),be(2),T()()),e&2){let t=Qe();O(2),Or(t.error)}}function n0(e,n){e&1&&(E(0,"th",21),be(1,"Date"),T())}function r0(e,n){if(e&1&&(E(0,"td",22),be(1),yd(2,"date"),T()),e&2){let t=n.$implicit;O(),Or(vd(2,1,t.date,"mediumDate"))}}function i0(e,n){e&1&&(E(0,"th",21),be(1,"Temperature (C)"),T())}function o0(e,n){if(e&1&&(E(0,"td",22),be(1),T()),e&2){let t=n.$implicit,r=Qe(2);K("hot-row",r.isHot(t.temperatureC)),O(),Ut(" ",t.temperatureC,"\xB0C ")}}function s0(e,n){e&1&&(E(0,"th",21),be(1,"Temperature (F)"),T())}function a0(e,n){if(e&1&&(E(0,"td",22),be(1),T()),e&2){let t=n.$implicit;O(),Ut("",t.temperatureF,"\xB0F")}}function c0(e,n){e&1&&(E(0,"th",21),be(1,"Summary"),T())}function l0(e,n){if(e&1&&(E(0,"td",22),be(1),T()),e&2){let t=n.$implicit;O(),Or(t.summary)}}function d0(e,n){e&1&&Le(0,"tr",23)}function u0(e,n){if(e&1&&Le(0,"tr",24),e&2){let t=n.$implicit,r=Qe(2);K("hot-row",r.isHot(t.temperatureC))}}function f0(e,n){if(e&1&&(E(0,"table",11),It(1,12),Pe(2,n0,2,0,"th",13)(3,r0,3,4,"td",14),Tt(),It(4,15),Pe(5,i0,2,0,"th",13)(6,o0,2,3,"td",16),Tt(),It(7,17),Pe(8,s0,2,0,"th",13)(9,a0,2,1,"td",14),Tt(),It(10,18),Pe(11,c0,2,0,"th",13)(12,l0,2,1,"td",14),Tt(),Pe(13,d0,1,0,"tr",19)(14,u0,1,2,"tr",20),T()),e&2){let t=Qe();Ce("dataSource",t.forecasts),O(13),Ce("matHeaderRowDef",t.displayedColumns),O(),Ce("matRowDefColumns",t.displayedColumns)}}var ga=class e{constructor(n,t){this.weatherService=n;this.cdr=t}weatherService;cdr;displayedColumns=["date","temperatureC","temperatureF","summary"];forecasts=[];loading=!0;error=null;ngOnInit(){this.loadForecasts()}loadForecasts(){this.loading=!0,this.error=null,this.weatherService.getForecasts().subscribe({next:n=>{this.forecasts=n,this.loading=!1,this.cdr.detectChanges()},error:n=>{this.error="Failed to load weather data. Please try again later.",this.loading=!1,this.cdr.detectChanges()}})}isHot(n){return n>30}static \u0275fac=function(t){return new(t||e)(xe(ma),xe(pn))};static \u0275cmp=P({type:e,selectors:[["app-weather"]],decls:12,vars:5,consts:[["appearance","outlined"],[4,"ngIf"],["class","loading-container",4,"ngIf"],["class","error-container",4,"ngIf"],["mat-table","","class","weather-table",3,"dataSource",4,"ngIf"],["align","end"],["mat-raised-button","","color","primary",3,"click","disabled"],[1,"loading-container"],["diameter","40"],[1,"error-container"],["color","warn","highlighted",""],["mat-table","",1,"weather-table",3,"dataSource"],["matColumnDef","date"],["mat-header-cell","",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["matColumnDef","temperatureC"],["mat-cell","",3,"hot-row",4,"matCellDef"],["matColumnDef","temperatureF"],["matColumnDef","summary"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",3,"hot-row",4,"matRowDef","matRowDefColumns"],["mat-header-cell",""],["mat-cell",""],["mat-header-row",""],["mat-row",""]],template:function(t,r){t&1&&(E(0,"mat-card",0)(1,"mat-card-header")(2,"mat-card-title"),be(3,"Weather Forecast"),T(),Pe(4,JE,2,1,"mat-card-subtitle",1),T(),E(5,"mat-card-content"),Pe(6,e0,4,0,"div",2)(7,t0,3,1,"div",3)(8,f0,15,3,"table",4),T(),E(9,"mat-card-actions",5)(10,"button",6),Un("click",function(){return r.loadForecasts()}),be(11," Refresh "),T()()()),t&2&&(O(4),Ce("ngIf",!r.loading&&!r.error),O(2),Ce("ngIf",r.loading),O(),Ce("ngIf",r.error),O(),Ce("ngIf",!r.loading&&!r.error),O(2),Ce("disabled",r.loading))},dependencies:[Vs,xd,Rg,_g,wg,Tg,Cg,Dg,Mg,Eg,Ig,Sg,xg,Lg,Ag,Fg,kg,Pg,Og,Ng,ry,ny,sy,oy,gy,my,Ad],styles:[".loading-container[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;padding:48px 0;gap:16px}.error-container[_ngcontent-%COMP%]{padding:16px 0}.weather-table[_ngcontent-%COMP%]{width:100%}.hot-row[_ngcontent-%COMP%]{background-color:#ffe0e0!important}.hot-row[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]{color:#c62828;font-weight:500}@media(max-width:600px){.weather-table[_ngcontent-%COMP%]{font-size:13px}.weather-table[_ngcontent-%COMP%]   .mat-mdc-header-cell[_ngcontent-%COMP%], .weather-table[_ngcontent-%COMP%]   .mat-mdc-cell[_ngcontent-%COMP%]{padding:8px 6px}}"]})};var ya=class e{static \u0275fac=function(t){return new(t||e)};static \u0275cmp=P({type:e,selectors:[["app-root"]],decls:2,vars:0,consts:[[1,"app-container"]],template:function(t,r){t&1&&(E(0,"div",0),Le(1,"app-weather"),T())},dependencies:[ga],styles:[".app-container[_ngcontent-%COMP%]{max-width:960px;margin:0 auto;padding:24px 16px}@media(max-width:600px){.app-container[_ngcontent-%COMP%]{padding:12px 8px}}"]})};Vd(ya,cg).catch(e=>console.error(e));
