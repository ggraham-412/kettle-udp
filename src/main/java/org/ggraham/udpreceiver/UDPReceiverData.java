package org.ggraham.udpreceiver;
/*

MIT License

Copyright (c) [2017] [Gregory Graham]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

import org.ggraham.nsr.message.PacketDecoder;
import org.ggraham.nsr.network.UDPReceiver;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.util.Date;

/**
 * 
 * Implements the PDI StepDataInterface
 * 
 * @author ggraham
 *
 */
public class UDPReceiverData extends BaseStepData implements StepDataInterface {

  // For output row meta 
  protected RowMetaInterface m_outputRowMeta;

  // UDP packet receiver and decoder
  protected UDPReceiver m_receiver;
  protected PacketDecoder m_decoder;
  
  // Implements packet counter stop mechanism 
  protected Object m_lock = new Object();   
  protected long m_packetCount;
  protected long m_currentPacketCount;
  
  // Implements packet receiver livetime stop mechanism
  protected long m_executionDuration;
  protected Date m_startTime;
}
