package com.hitachivantara.udpsender;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.userdefinedjavaclass.FieldHelper;

import com.hitachivantara.message.PacketDecoder;
import com.hitachivantara.network.UDPReceiver;
import com.hitachivantara.network.UDPSender__;
import com.hitachivantara.network.UDPSender;

import java.util.Date;
import java.util.HashMap;

public class UDPSenderData extends BaseStepData implements StepDataInterface {
 
  HashMap<Integer, Integer> m_fieldMap;
  protected RowMetaInterface m_inputRowMeta;
  protected UDPSender m_sender;
  protected PacketDecoder m_decoder;
}
