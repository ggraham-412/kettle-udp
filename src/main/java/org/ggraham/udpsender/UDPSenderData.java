package org.ggraham.udpsender;
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

import org.ggraham.message.PacketDecoder;
import org.ggraham.network.UDPReceiver;
import org.ggraham.network.UDPSender;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.userdefinedjavaclass.FieldHelper;

import java.util.Date;
import java.util.HashMap;

/**
 * 
 * Implements the PDI StepDataInterface
 * 
 * @author ggraham
 *
 */
public class UDPSenderData extends BaseStepData implements StepDataInterface {

	// For input row meta
	protected RowMetaInterface m_inputRowMeta;

	// UDP Sender and packet encoder 
	protected UDPSender m_sender;
	protected PacketDecoder m_decoder;
	
	// Allows for dynamic re-ordering of fields so that row order 
	// does not have to match the field order in the packet
	HashMap<Integer, Integer> m_fieldMap;
}
