use std::fmt::Debug;

use anyhow::Result;

use crate::common::types::{DataType, DataValue};
use crate::expr::expr::Operator;
use crate::physical_expr::physical_expr::eval_binary_numeric_value_pair;

pub trait Accumulator: Send + Sync + Debug {
    fn update_batch(&mut self, values: &[&[DataValue]]) -> Result<()>;
    fn evaluate(&self) -> Result<DataValue>;
}

#[derive(Debug)]
pub struct SumAccumulator {
    sum: DataValue,
}

impl SumAccumulator {
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            sum: DataValue::try_from(data_type)?,
        })
    }
}

impl Accumulator for SumAccumulator {
    fn update_batch(&mut self, values: &[&[DataValue]]) -> Result<()> {
        let values = values[0];
        let mut initiated = !self.sum.is_none();
        for value in values {
            if initiated == true {
                self.sum = eval_binary_numeric_value_pair(&self.sum, value, Operator::Plus)?;
            } else {
                self.sum = value.clone();
                initiated = true;
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<DataValue> {
        Ok(self.sum.clone())
    }
}

#[derive(Debug)]
pub struct MaxAccumulator {
    max: DataValue,
}

impl MaxAccumulator {
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            max: DataValue::try_from(data_type)?,
        })
    }
}

impl Accumulator for MaxAccumulator {
    fn update_batch(&mut self, values: &[&[DataValue]]) -> Result<()> {
        let values = values[0];
        let mut initiated = !self.max.is_none();
        for value in values {
            if initiated == true {
                if *value > self.max {
                    self.max = value.clone();
                }
            } else {
                self.max = value.clone();
                initiated = true;
            }
        }
        Ok(())
    }
    fn evaluate(&self) -> Result<DataValue> {
        Ok(self.max.clone())
    }
}

#[derive(Debug)]
pub struct MinAccumulator {
    min: DataValue,
}

impl MinAccumulator {
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            min: DataValue::try_from(data_type)?,
        })
    }
}

impl Accumulator for MinAccumulator {
    fn update_batch(&mut self, values: &[&[DataValue]]) -> Result<()> {
        let values = values[0];
        let mut initiated = !self.min.is_none();
        for value in values {
            if initiated == true {
                if *value < self.min {
                    self.min = value.clone();
                }
            } else {
                self.min = value.clone();
                initiated = true;
            }
        }
        Ok(())
    }
    fn evaluate(&self) -> Result<DataValue> {
        Ok(self.min.clone())
    }
}

#[derive(Debug)]
pub struct CountAccumulator {
    count: i64,
}

impl CountAccumulator {
    pub fn new() -> Result<Self> {
        Ok(Self { count: 0 })
    }
}

impl Accumulator for CountAccumulator {
    fn update_batch(&mut self, values: &[&[DataValue]]) -> Result<()> {
        let total_row = values[0].len();
        let mut null_row = 0_usize;
        // for a given row, if any column is NULL, it is seem as NULL
        for &v_vec in values {
            let mut is_null = false;
            for v in v_vec {
                if v.is_null() || v.is_none() {
                    is_null = true;
                    break;
                }
            }
            if is_null {
                null_row += 1;
            }
        }
        self.count += (total_row - null_row) as i64;
        Ok(())
    }
    fn evaluate(&self) -> Result<DataValue> {
        Ok(DataValue::Int64(Some(self.count)))
    }
}

#[derive(Debug)]
pub struct AvgAccumulator {
    sum: DataValue,
    count: i64,
}

impl AvgAccumulator {
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            sum: DataValue::try_from(data_type)?,
            count: 0,
        })
    }
}

impl Accumulator for AvgAccumulator {
    fn update_batch(&mut self, values: &[&[DataValue]]) -> Result<()> {
        let values = values[0];
        let mut initiated = !self.sum.is_none();
        for value in values {
            if initiated == true {
                self.sum = eval_binary_numeric_value_pair(&self.sum, value, Operator::Plus)?;
            } else {
                self.sum = value.clone();
                initiated = true;
            }
            self.count += 1;
        }
        Ok(())
    }
    fn evaluate(&self) -> Result<DataValue> {
        eval_binary_numeric_value_pair(
            &self.sum,
            &DataValue::Int64(Some(self.count)),
            Operator::Divide,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_sum_accumulator() -> Result<()> {
        let mut mysum1 = SumAccumulator::try_new(&DataType::Float32)?;
        mysum1.update_batch(&[&[DataValue::Float32(Some(1.0)), DataValue::Float32(Some(3.0))]])?;
        assert_eq!(mysum1.evaluate()?, DataValue::Float32(Some(4.0)));
        let mut mysum2 = SumAccumulator::try_new(&DataType::Float32)?;
        mysum2.update_batch(&[&[
            DataValue::Float32(Some(1.0)),
            DataValue::Float32(Some(3.0)),
            DataValue::Float32(Some(5.0)),
        ]])?;
        assert_eq!(mysum2.evaluate()?, DataValue::Float32(Some(9.0)));
        Ok(())
    }
    #[test]
    fn test_max_accumulator() -> Result<()> {
        let mut mymax1 = MaxAccumulator::try_new(&DataType::Float32)?;
        mymax1.update_batch(&[&[DataValue::Float32(Some(1.0)), DataValue::Float32(Some(3.0))]])?;
        assert_eq!(mymax1.evaluate()?, DataValue::Float32(Some(3.0)));
        let mut mymax2 = MaxAccumulator::try_new(&DataType::Float32)?;
        mymax2.update_batch(&[&[
            DataValue::Float32(Some(1.0)),
            DataValue::Float32(Some(3.0)),
            DataValue::Float32(Some(5.0)),
        ]])?;
        assert_eq!(mymax2.evaluate()?, DataValue::Float32(Some(5.0)));
        Ok(())
    }
    #[test]
    fn test_min_accumulator() -> Result<()> {
        let mut mymin1 = MinAccumulator::try_new(&DataType::Float32)?;
        mymin1.update_batch(&[&[DataValue::Float32(Some(1.0)), DataValue::Float32(Some(3.0))]])?;
        assert_eq!(mymin1.evaluate()?, DataValue::Float32(Some(1.0)));
        let mut mymin2 = MinAccumulator::try_new(&DataType::Float32)?;
        mymin2.update_batch(&[&[
            DataValue::Float32(Some(2.0)),
            DataValue::Float32(Some(3.0)),
            DataValue::Float32(Some(5.0)),
        ]])?;
        assert_eq!(mymin2.evaluate()?, DataValue::Float32(Some(2.0)));
        Ok(())
    }
    #[test]
    fn test_count_accumulator() -> Result<()> {
        let mut mycount = CountAccumulator::new()?;
        mycount.update_batch(&[
            &[DataValue::Float32(Some(1.0)), DataValue::Float32(Some(3.0))],
            &[DataValue::Float32(Some(1.0)), DataValue::Float32(Some(3.0))],
        ])?;
        assert_eq!(mycount.evaluate()?, DataValue::Int64(Some(2)));
        Ok(())
    }
    #[test]
    fn test_avg_accumulator() -> Result<()> {
        let mut myavg = AvgAccumulator::try_new(&DataType::Float32)?;
        myavg.update_batch(&[&[DataValue::Float32(Some(1.0)), DataValue::Float32(Some(4.0))]])?;
        assert_eq!(myavg.evaluate()?, DataValue::Float32(Some(2.5)));
        Ok(())
    }
}
