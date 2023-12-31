package org.partiql.jdbc;

import org.partiql.lang.eval.*;

import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/* TODO: There has to be a better way to do this */
public class PartiQLGlobalBindings {
    private ArrayList<String> knownNames;
    private Bindings<ExprValue> bindings;
    private ExprValueFactory valueFactory;

    protected PartiQLGlobalBindings(ExprValueFactory valueFactory) {
        this.valueFactory = valueFactory;
        this.knownNames = new ArrayList<String>();
        this.bindings = Bindings.Companion.empty();
    }

    PartiQLGlobalBindings addBinding(Bindings<ExprValue> bindings) {
        if (bindings instanceof MapBindings) {
            this.knownNames.addAll(((MapBindings<ExprValue>) bindings).getOriginalCaseMap().keySet());
            this.bindings = BindingsExtensionsKt.delegate(bindings, this.bindings);
        } else {
            // nothing to do
        }
        return this;
    }

    ExprValue asExprValue() {
        Stream<ExprValue> values = this.knownNames.stream()
                                             .map(value -> this.bindings.get(new BindingName(value, BindingCase.SENSITIVE)))
                                             .filter(Objects::nonNull);
        return this.valueFactory.newStruct(values.collect(Collectors.toList()), StructOrdering.ORDERED);
    }
}
