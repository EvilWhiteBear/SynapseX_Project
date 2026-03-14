/**
 * SYNAPSE X // TERMINAL CORE LOGIC
 */

document.addEventListener('DOMContentLoaded', () => {
    const sendBtn = document.getElementById('send-btn');
    const chatInput = document.getElementById('chat-input');
    const chatBox = document.getElementById('chat-box');
    const levInp = document.getElementById('lev');
    const margInp = document.getElementById('marg');
    const posSizeDisplay = document.getElementById('pos-size');

    const appendMsg = (content, type = 'u') => {
        const msgDiv = document.createElement('div');
        msgDiv.className = `msg msg-${type}`;
        
        if (type === 'a') {
            // Очистка от входящих тегов <br> и замена на \n
            let cleanText = content.replace(/<br\s*\/?>/gi, '\n');
            
            // Создаем премиальный контейнер
            const container = document.createElement('div');
            container.className = 'neural-response';
            
            // Обработка текста: подсветка заголовков и ключевых слов
            let formatted = cleanText
                .replace(/(АНАЛИЗ ПАРЫ|РЕКОМЕНДАЦИЯ|ОБОСНОВАНИЕ|ANALYSIS|RECOMMENDATION|RATIONALE):/gi, 
                         '<span class="section-header">$1:</span>')
                .replace(/\b(LONG|BUY|ЛОНГ|ПОКУПКА)\b/g, '<span class="keyword-long">$1</span>')
                .replace(/\b(SHORT|SELL|ШОРТ|ПРОДАЖА)\b/g, '<span class="keyword-short">$1</span>');

            container.innerHTML = formatted;
            msgDiv.appendChild(container);
        } else {
            msgDiv.innerText = content;
        }

        chatBox.appendChild(msgDiv);
        chatBox.scrollTop = chatBox.scrollHeight;
    };

    window.sendMsg = async () => {
        const text = chatInput.value.trim();
        if (!text) return;

        appendMsg(text, 'u');
        chatInput.value = '';

        try {
            const response = await fetch('/ask_ai', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: text,
                    asset: window.currentAsset || "SOL-USDT",
                    leverage: levInp.value,
                    margin: margInp.value
                })
            });
            const data = await response.json();
            
            if (data.raw_signal) {
                const sig = data.raw_signal;
                const riskHtml = `
                    <div class="risk-calc-block">
                        <span class="risk-header">--- RISK ASSESSMENT ---</span>
                        DIRECTION: ${sig.direction}\nENTRY: ${sig.entry}\nTP1: ${sig.tp1}\nSL: ${sig.sl}
                    </div>`;
                appendMsg(riskHtml + "\n" + data.answer, 'a');
            } else {
                appendMsg(data.answer, 'a');
            }
        } catch (err) {
            appendMsg("SYSTEM_FAILURE: Link lost.", 'a');
        }
    };

    if (sendBtn) sendBtn.onclick = window.sendMsg;
    if (chatInput) {
        chatInput.onkeypress = (e) => { if (e.key === 'Enter') window.sendMsg(); };
    }

    const updatePos = () => {
        const size = (parseFloat(levInp.value) || 0) * (parseFloat(margInp.value) || 0);
        if (posSizeDisplay) posSizeDisplay.innerText = `$${size.toFixed(2)}`;
    };
    levInp.oninput = updatePos;
    margInp.oninput = updatePos;
});