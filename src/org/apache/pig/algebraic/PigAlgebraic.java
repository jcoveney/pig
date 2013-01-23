package org.apache.pig.algebraic;

import org.apache.pig.AlgebraicEvalFunc;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class PigAlgebraic<InterT, FinT> extends AlgebraicEvalFunc<FinT> {
    private AlgebraicFuncSpec<PigInitial<InterT>> getInitialAlgebraicFuncSpec() {
        return new AlgebraicFuncSpec<PigInitial<InterT>>(getInitialClass());
    }
    private AlgebraicFuncSpec<PigIntermed<InterT>> getIntermedAlgebraicFuncSpec() {
        return new AlgebraicFuncSpec<PigIntermed<InterT>>(getIntermedClass());
    }
    private AlgebraicFuncSpec<PigFinal<InterT,FinT>> getFinalAlgebraicFuncSpec() {
        return new AlgebraicFuncSpec<PigFinal<InterT,FinT>>(getFinalClass());
    }

    public abstract Class<? extends PigInitial<InterT>> getInitialClass();
    public abstract Class<? extends PigIntermed<InterT>> getIntermedClass();
    public abstract Class<? extends PigFinal<InterT,FinT>> getFinalClass();

    @Override
    public String getInitial() {
        return getInitialAlgebraicFuncSpec().getFuncString();
    }

    @Override
    public String getIntermed() {
        return getIntermedAlgebraicFuncSpec().getFuncString();
    }

    @Override
    public String getFinal() {
        return getFinalAlgebraicFuncSpec().getFuncString();
    }
}
