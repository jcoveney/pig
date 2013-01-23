package org.apache.pig.algebraic;

import org.apache.pig.AlgebraicEvalFunc;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class PigAlgebraic<InterT, FinT> extends AlgebraicEvalFunc<FinT> {
    public abstract AlgebraicFuncSpec<PigInitial<InterT>> getInitialClass();
    public abstract AlgebraicFuncSpec<PigIntermed<InterT>> getIntermedClass();
    public abstract AlgebraicFuncSpec<PigFinal<InterT,FinT>> getFinalClass();

    @Override
    public String getInitial() {
        return getInitialClass().getFuncString();
    }

    @Override
    public String getIntermed() {
        return getIntermedClass().getFuncString();
    }

    @Override
    public String getFinal() {
        return getFinalClass().getFuncString();
    }
}
