function bindTo(target, source) {
    var descriptors = Object.getOwnPropertyDescriptors(source);
    for (var key in descriptors) {
        if (!(key[0].toUpperCase() === key[0])) {
            var descriptor = descriptors[key];
            if (typeof (descriptor.value) === 'function') {
                descriptor.value = descriptor.value.bind(source)
            } else if (typeof (descriptor.get) === 'function') {
                descriptor.get = descriptor.get.bind(source);
            }
        }
    }
    Object.defineProperties(target, descriptors);
}

var Scope = function () {
};
Scope.prototype = self;
var $doc, $entry, $moduleName, $moduleBase;
var $wnd = new Scope();
bindTo($wnd, self);

var dh = {}
$wnd.dh = dh;

var $gwt_version = "__GWT_VERSION__";

__JAVASCRIPT_RESULT__

gwtOnLoad(null, '__MODULE_NAME__', null);
export {dh};
